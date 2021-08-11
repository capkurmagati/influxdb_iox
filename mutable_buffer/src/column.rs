use std::convert::TryInto;
use std::mem;
use std::sync::Arc;

use arrow::{
    array::{
        ArrayDataBuilder, ArrayRef, BooleanArray, Float64Array, Int64Array,
        TimestampNanosecondArray, UInt64Array,
    },
    datatypes::DataType,
};
use snafu::{ensure, Snafu};

use arrow_util::bitset::{iter_set_positions, BitSet};
use arrow_util::string::PackedStringArray;
use data_types::partition_metadata::{IsNan, StatValues, Statistics};
use entry::Column as EntryColumn;
use internal_types::schema::{IOxValueType, InfluxColumnType, InfluxFieldType, TIME_DATA_TYPE};

use crate::dictionary::{Dictionary, DID, INVALID_DID};

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)]
pub enum Error {
    #[snafu(display("Unable to insert {} type into a column of {}", inserted, existing,))]
    TypeMismatch {
        existing: InfluxColumnType,
        inserted: InfluxColumnType,
    },

    #[snafu(display(
        "Invalid null mask, expected to be {} bytes but was {}",
        expected_bytes,
        actual_bytes
    ))]
    InvalidNullMask {
        expected_bytes: usize,
        actual_bytes: usize,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug)]
pub struct Column {
    influx_type: InfluxColumnType,
    valid: BitSet,
    data: ColumnData,
}

#[derive(Debug)]
pub enum ColumnData {
    F64(Vec<f64>, StatValues<f64>),
    I64(Vec<i64>, StatValues<i64>),
    U64(Vec<u64>, StatValues<u64>),
    String(PackedStringArray<i32>, StatValues<String>),
    Bool(BitSet, StatValues<bool>),
    Tag(Vec<DID>, Dictionary, StatValues<String>),
}

impl Column {
    pub fn new(row_count: usize, column_type: InfluxColumnType) -> Self {
        let mut valid = BitSet::new();
        valid.append_unset(row_count);

        // Keep track of how many total rows there are
        let total_count = row_count as u64;

        let data = match column_type {
            InfluxColumnType::IOx(IOxValueType::Boolean)
            | InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                let mut data = BitSet::new();
                data.append_unset(row_count);
                ColumnData::Bool(data, StatValues::new_all_null(total_count))
            }
            InfluxColumnType::IOx(IOxValueType::U64)
            | InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                ColumnData::U64(vec![0; row_count], StatValues::new_all_null(total_count))
            }
            InfluxColumnType::IOx(IOxValueType::F64)
            | InfluxColumnType::Field(InfluxFieldType::Float) => {
                ColumnData::F64(vec![0.0; row_count], StatValues::new_all_null(total_count))
            }
            InfluxColumnType::IOx(IOxValueType::I64)
            | InfluxColumnType::Field(InfluxFieldType::Integer)
            | InfluxColumnType::Timestamp => {
                ColumnData::I64(vec![0; row_count], StatValues::new_all_null(total_count))
            }
            InfluxColumnType::IOx(IOxValueType::String)
            | InfluxColumnType::Field(InfluxFieldType::String) => ColumnData::String(
                PackedStringArray::new_empty(row_count),
                StatValues::new_all_null(total_count),
            ),
            InfluxColumnType::Tag => ColumnData::Tag(
                vec![INVALID_DID; row_count],
                Default::default(),
                StatValues::new_all_null(total_count),
            ),
            InfluxColumnType::IOx(IOxValueType::Bytes) => todo!(),
        };

        Self {
            influx_type: column_type,
            valid,
            data,
        }
    }

    pub fn validate_schema(&self, entry: &EntryColumn<'_>) -> Result<()> {
        let entry_type = entry.influx_type();

        ensure!(
            entry_type == self.influx_type,
            TypeMismatch {
                existing: self.influx_type,
                inserted: entry_type
            }
        );

        Ok(())
    }

    pub fn influx_type(&self) -> InfluxColumnType {
        self.influx_type
    }

    pub fn append(
        &mut self,
        entry: &EntryColumn<'_>,
        inclusion_mask: Option<&[bool]>,
    ) -> Result<()> {
        self.validate_schema(entry)?;

        let masked_values = if let Some(mask) = inclusion_mask {
            assert_eq!(entry.row_count, mask.len());
            mask.iter().map(|x| !x as usize).sum::<usize>()
        } else {
            0
        };
        let row_count = entry.row_count - masked_values;
        if row_count == 0 {
            return Ok(());
        }

        let valid_mask = construct_valid_mask(entry, row_count, inclusion_mask)?;

        match &mut self.data {
            ColumnData::Bool(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_bool_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload");

                let data_offset = col_data.len();
                col_data.append_unset(row_count);

                let initial_total_count = stats.total_count;
                let mut added = 0;

                for (idx, value) in iter_set_positions(&valid_mask)
                    .zip(MaskedIter::new(entry_data.iter(), inclusion_mask))
                {
                    stats.update(value);

                    if *value {
                        col_data.set(data_offset + idx);
                    }

                    added += 1;
                }

                let null_count = row_count - added;
                stats.update_for_nulls(null_count as u64);

                assert_eq!(
                    stats.total_count - initial_total_count - null_count as u64,
                    added as u64
                );
            }
            ColumnData::U64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_u64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(
                    row_count,
                    &valid_mask,
                    entry_data,
                    col_data,
                    stats,
                    inclusion_mask,
                );
            }
            ColumnData::F64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_f64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(
                    row_count,
                    &valid_mask,
                    entry_data,
                    col_data,
                    stats,
                    inclusion_mask,
                );
            }
            ColumnData::I64(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_i64values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload")
                    .into_iter();

                handle_write(
                    row_count,
                    &valid_mask,
                    entry_data,
                    col_data,
                    stats,
                    inclusion_mask,
                );
            }
            ColumnData::String(col_data, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_string_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload");

                let data_offset = col_data.len();
                let initial_total_count = stats.total_count;
                let mut added = 0;

                for (str, idx) in MaskedIter::new(entry_data.iter(), inclusion_mask)
                    .zip(iter_set_positions(&valid_mask))
                {
                    col_data.extend(data_offset + idx - col_data.len());
                    stats.update(str);
                    col_data.append(str);
                    added += 1;
                }
                col_data.extend(data_offset + row_count - col_data.len());

                let null_count = row_count - added;
                stats.update_for_nulls(null_count as u64);

                assert_eq!(
                    stats.total_count - initial_total_count - null_count as u64,
                    added as u64
                );
            }
            ColumnData::Tag(col_data, dictionary, stats) => {
                let entry_data = entry
                    .inner()
                    .values_as_string_values()
                    .expect("invalid flatbuffer")
                    .values()
                    .expect("invalid payload");

                let data_offset = col_data.len();
                col_data.resize(data_offset + row_count, INVALID_DID);

                let initial_total_count = stats.total_count;
                let mut added = 0;

                for (idx, value) in iter_set_positions(&valid_mask)
                    .zip(MaskedIter::new(entry_data.iter(), inclusion_mask))
                {
                    stats.update(value);
                    col_data[data_offset + idx] = dictionary.lookup_value_or_insert(value);
                    added += 1;
                }

                let null_count = row_count - added;
                stats.update_for_nulls(null_count as u64);

                assert_eq!(
                    stats.total_count - initial_total_count - null_count as u64,
                    added as u64
                );
            }
        };

        self.valid.append_bits(row_count, &valid_mask);
        Ok(())
    }

    /// Ensures that the total length of this column is `len` rows,
    /// padding it with trailing NULLs if necessary
    pub fn push_nulls_to_len(&mut self, len: usize) {
        if self.valid.len() == len {
            return;
        }
        assert!(len > self.valid.len(), "cannot shrink column");
        let delta = len - self.valid.len();
        self.valid.append_unset(delta);

        match &mut self.data {
            ColumnData::F64(data, stats) => {
                data.resize(len, 0.);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::I64(data, stats) => {
                data.resize(len, 0);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::U64(data, stats) => {
                data.resize(len, 0);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::String(data, stats) => {
                data.extend(delta);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::Bool(data, stats) => {
                data.append_unset(delta);
                stats.update_for_nulls(delta as u64);
            }
            ColumnData::Tag(data, _dict, stats) => {
                data.resize(len, INVALID_DID);
                stats.update_for_nulls(delta as u64);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.valid.len()
    }

    pub fn stats(&self) -> Statistics {
        match &self.data {
            ColumnData::F64(_, stats) => Statistics::F64(stats.clone()),
            ColumnData::I64(_, stats) => Statistics::I64(stats.clone()),
            ColumnData::U64(_, stats) => Statistics::U64(stats.clone()),
            ColumnData::Bool(_, stats) => Statistics::Bool(stats.clone()),
            ColumnData::String(_, stats) => Statistics::String(stats.clone()),
            ColumnData::Tag(_, dictionary, stats) => {
                let mut distinct_count = dictionary.values().len() as u64;
                if stats.null_count > 0 {
                    distinct_count += 1;
                }

                let mut stats = stats.clone();
                stats.distinct_count = distinct_count.try_into().ok();
                Statistics::String(stats)
            }
        }
    }

    /// The approximate memory size of the data in the column. Note that
    /// the space taken for the tag string values is represented in
    /// the dictionary size in the chunk that holds the table that has this
    /// column. The size returned here is only for their identifiers.
    pub fn size(&self) -> usize {
        let data_size = match &self.data {
            ColumnData::F64(v, stats) => mem::size_of::<f64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::I64(v, stats) => mem::size_of::<i64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::U64(v, stats) => mem::size_of::<u64>() * v.len() + mem::size_of_val(&stats),
            ColumnData::Bool(v, stats) => v.byte_len() + mem::size_of_val(&stats),
            ColumnData::Tag(v, dictionary, stats) => {
                mem::size_of::<DID>() * v.len() + dictionary.size() + mem::size_of_val(&stats)
            }
            ColumnData::String(v, stats) => {
                v.size() + mem::size_of_val(&stats) + stats.string_size()
            }
        };
        data_size + self.valid.byte_len()
    }

    pub fn to_arrow(&self) -> Result<ArrayRef> {
        let nulls = self.valid.to_arrow();
        let data: ArrayRef = match &self.data {
            ColumnData::F64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Float64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(Float64Array::from(data))
            }
            ColumnData::I64(data, _) => match self.influx_type {
                InfluxColumnType::Timestamp => {
                    let data = ArrayDataBuilder::new(TIME_DATA_TYPE())
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();
                    Arc::new(TimestampNanosecondArray::from(data))
                }
                InfluxColumnType::IOx(IOxValueType::I64)
                | InfluxColumnType::Field(InfluxFieldType::Integer) => {
                    let data = ArrayDataBuilder::new(DataType::Int64)
                        .len(data.len())
                        .add_buffer(data.iter().cloned().collect())
                        .null_bit_buffer(nulls)
                        .build();

                    Arc::new(Int64Array::from(data))
                }
                _ => unreachable!(),
            },
            ColumnData::U64(data, _) => {
                let data = ArrayDataBuilder::new(DataType::UInt64)
                    .len(data.len())
                    .add_buffer(data.iter().cloned().collect())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(UInt64Array::from(data))
            }
            ColumnData::String(data, _) => Arc::new(data.to_arrow()),
            ColumnData::Bool(data, _) => {
                let data = ArrayDataBuilder::new(DataType::Boolean)
                    .len(data.len())
                    .add_buffer(data.to_arrow())
                    .null_bit_buffer(nulls)
                    .build();
                Arc::new(BooleanArray::from(data))
            }
            ColumnData::Tag(data, dictionary, _) => {
                Arc::new(dictionary.to_arrow(data.iter().cloned(), Some(nulls)))
            }
        };

        assert_eq!(data.len(), self.len());

        Ok(data)
    }
}

struct MaskedIter<'a, I> {
    it: I,
    mask: Option<core::slice::Iter<'a, bool>>,
}

impl<'a, I> MaskedIter<'a, I> {
    fn new(it: I, mask: Option<&'a [bool]>) -> Self {
        Self {
            it,
            mask: mask.map(|mask| mask.iter()),
        }
    }
}

impl<'a, I> Iterator for MaskedIter<'a, I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mask) = self.mask.as_mut() {
            for included in mask {
                // always query `it` so that masked elements are skipped
                let res = self.it.next();
                if *included {
                    return res;
                }
            }

            None
        } else {
            self.it.next()
        }
    }
}

/// Construct a validity mask from the given column's null mask
fn construct_valid_mask(
    column: &EntryColumn<'_>,
    row_count: usize,
    inclusion_mask: Option<&[bool]>,
) -> Result<Vec<u8>> {
    let buf_len_in = (column.row_count + 7) >> 3;
    let buf_len_out = (row_count + 7) >> 3;

    match column.inner().null_mask() {
        Some(data) => {
            ensure!(
                data.len() == buf_len_in,
                InvalidNullMask {
                    expected_bytes: buf_len_in,
                    actual_bytes: data.len()
                }
            );

            let res: Vec<u8> = if let Some(inclusion_mask) = inclusion_mask {
                let mut out_byte: u8 = 0;
                let mut out_bit_counter = 0;

                let mut in_byte: u8 = 0;
                let mut in_bit_counter = 8;

                let mut it = data.iter();
                let mut out: Vec<u8> = Vec::with_capacity(buf_len_out);

                for include in inclusion_mask {
                    // do we need to read the next byte?
                    if in_bit_counter == 8 {
                        out_byte = 0;
                        in_byte = *it.next().expect("checked length before");
                        in_bit_counter = 0;
                    }

                    // read bit from input byte
                    let bit = in_byte & 0x1;
                    in_byte >>= 1;
                    in_bit_counter += 1;

                    // flip bit (null => validity)
                    let bit = (!bit) & 0x1;

                    if *include {
                        // out byte full?
                        if out_bit_counter == 8 {
                            out.push(out_byte);
                            out_byte = 0;
                            out_bit_counter = 0;
                        }

                        // append bit to out byte
                        out_byte |= bit << out_bit_counter;
                        out_bit_counter += 1;
                    }
                }
                if out_bit_counter > 0 {
                    out.push(out_byte);
                }

                out
            } else {
                data.iter()
                    .map(|x| {
                        // Currently the bit mask is backwards
                        !x.reverse_bits()
                    })
                    .collect()
            };
            assert_eq!(res.len(), buf_len_out);

            Ok(res)
        }
        None => {
            // If no null mask they're all valid
            let mut data = Vec::new();
            data.resize(buf_len_out, 0xFF);
            Ok(data)
        }
    }
}

/// Writes entry data into a column based on the valid mask
fn handle_write<T, E>(
    row_count: usize,
    valid_mask: &[u8],
    entry_data: E,
    col_data: &mut Vec<T>,
    stats: &mut StatValues<T>,
    inclusion_mask: Option<&[bool]>,
) where
    T: Clone + Default + PartialOrd + IsNan,
    E: Iterator<Item = T> + ExactSizeIterator,
{
    let data_offset = col_data.len();
    col_data.resize(data_offset + row_count, Default::default());

    let initial_total_count = stats.total_count;
    let mut added = 0;

    for (idx, value) in
        iter_set_positions(valid_mask).zip(MaskedIter::new(entry_data, inclusion_mask))
    {
        stats.update(&value);
        col_data[data_offset + idx] = value;
        added += 1;
    }

    let null_count = row_count - added;
    stats.update_for_nulls(null_count as u64);

    assert_eq!(
        stats.total_count - initial_total_count - null_count as u64,
        added as u64
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_masked_iterator() {
        let actual: Vec<u32> = MaskedIter::new(IntoIterator::into_iter([]), None).collect();
        let expected: Vec<u32> = vec![];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(IntoIterator::into_iter([]), Some(&[])).collect();
        let expected: Vec<u32> = vec![];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(IntoIterator::into_iter([1, 2, 3]), None).collect();
        let expected: Vec<u32> = vec![1, 2, 3];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(
            IntoIterator::into_iter([1, 2, 3]),
            Some(&[true, true, true]),
        )
        .collect();
        let expected: Vec<u32> = vec![1, 2, 3];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(
            IntoIterator::into_iter([1, 2, 3]),
            Some(&[false, false, false]),
        )
        .collect();
        let expected: Vec<u32> = vec![];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(
            IntoIterator::into_iter([1, 2, 3]),
            Some(&[true, true, false]),
        )
        .collect();
        let expected: Vec<u32> = vec![1, 2];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(
            IntoIterator::into_iter([1, 2, 3]),
            Some(&[true, false, true]),
        )
        .collect();
        let expected: Vec<u32> = vec![1, 3];
        assert_eq!(actual, expected);

        let actual: Vec<u32> = MaskedIter::new(
            IntoIterator::into_iter([1, 2, 3]),
            Some(&[false, true, false]),
        )
        .collect();
        let expected: Vec<u32> = vec![2];
        assert_eq!(actual, expected);
    }
}

use std::convert::{TryFrom, TryInto};

use serde::{de::Visitor, ser::SerializeStruct, Deserialize, Serialize};
use serde_bytes::{ByteBuf, Bytes};

use super::{Error, RawArray, RawDocument, Result};
use crate::{
    de::convert_unsigned_to_signed_raw,
    extjson,
    oid::{self, ObjectId},
    raw::{RAW_ARRAY_NEWTYPE, RAW_BSON_NEWTYPE, RAW_DOCUMENT_NEWTYPE},
    spec::{BinarySubtype, ElementType},
    Bson,
    DateTime,
    Decimal128,
    Timestamp,
};

/// A BSON value referencing raw bytes stored elsewhere.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RawBson<'a> {
    /// 64-bit binary floating point
    Double(f64),
    /// UTF-8 string
    String(&'a str),
    /// Array
    Array(&'a RawArray),
    /// Embedded document
    Document(&'a RawDocument),
    /// Boolean value
    Boolean(bool),
    /// Null value
    Null,
    /// 32-bit signed integer
    Int32(i32),
    /// 64-bit signed integer
    Int64(i64),
    /// 32-bit signed integer
    UInt32(u32),
    /// 64-bit signed integer
    UInt64(u64),
    /// Timestamp
    Timestamp(Timestamp),
    /// Binary data
    Binary(RawBinary<'a>),
    /// UTC datetime
    DateTime(crate::DateTime),
    /// [128-bit decimal floating point](https://github.com/mongodb/specifications/blob/master/source/bson-decimal128/decimal128.rst)
    Decimal128(Decimal128),
}

impl<'a> RawBson<'a> {
    /// Get the [`ElementType`] of this value.
    pub fn element_type(&self) -> ElementType {
        match *self {
            RawBson::Double(..) => ElementType::Double,
            RawBson::String(..) => ElementType::String,
            RawBson::Array(..) => ElementType::Array,
            RawBson::Document(..) => ElementType::EmbeddedDocument,
            RawBson::Boolean(..) => ElementType::Boolean,
            RawBson::Null => ElementType::Null,
            RawBson::Int32(..) => ElementType::Int32,
            RawBson::Int64(..) => ElementType::Int64,
            RawBson::UInt32(..) => ElementType::UInt32,
            RawBson::UInt64(..) => ElementType::UInt64,
            RawBson::Timestamp(..) => ElementType::Timestamp,
            RawBson::Binary(..) => ElementType::Binary,
            RawBson::DateTime(..) => ElementType::DateTime,
            RawBson::Decimal128(..) => ElementType::Decimal128,
        }
    }

    /// Gets the `f64` that's referenced or returns `None` if the referenced value isn't a BSON
    /// double.
    pub fn as_f64(self) -> Option<f64> {
        match self {
            RawBson::Double(d) => Some(d),
            _ => None,
        }
    }

    /// Gets the `&str` that's referenced or returns `None` if the referenced value isn't a BSON
    /// String.
    pub fn as_str(self) -> Option<&'a str> {
        match self {
            RawBson::String(s) => Some(s),
            _ => None,
        }
    }

    /// Gets the [`RawArray`] that's referenced or returns `None` if the referenced value
    /// isn't a BSON array.
    pub fn as_array(self) -> Option<&'a RawArray> {
        match self {
            RawBson::Array(v) => Some(v),
            _ => None,
        }
    }

    /// Gets the [`RawDocument`] that's referenced or returns `None` if the referenced value
    /// isn't a BSON document.
    pub fn as_document(self) -> Option<&'a RawDocument> {
        match self {
            RawBson::Document(v) => Some(v),
            _ => None,
        }
    }

    /// Gets the `bool` that's referenced or returns `None` if the referenced value isn't a BSON
    /// boolean.
    pub fn as_bool(self) -> Option<bool> {
        match self {
            RawBson::Boolean(v) => Some(v),
            _ => None,
        }
    }

    /// Gets the `i32` that's referenced or returns `None` if the referenced value isn't a BSON
    /// Int32.
    pub fn as_i32(self) -> Option<i32> {
        match self {
            RawBson::Int32(v) => Some(v),
            _ => None,
        }
    }

    /// Gets the `i64` that's referenced or returns `None` if the referenced value isn't a BSON
    /// Int64.
    pub fn as_i64(self) -> Option<i64> {
        match self {
            RawBson::Int64(v) => Some(v),
            _ => None,
        }
    }


    /// Gets the [`RawBinary`] that's referenced or returns `None` if the referenced value isn't a
    /// BSON binary.
    pub fn as_binary(self) -> Option<RawBinary<'a>> {
        match self {
            RawBson::Binary(v) => Some(v),
            _ => None,
        }
    }


    /// Gets the [`crate::DateTime`] that's referenced or returns `None` if the referenced value
    /// isn't a BSON datetime.
    pub fn as_datetime(self) -> Option<crate::DateTime> {
        match self {
            RawBson::DateTime(v) => Some(v),
            _ => None,
        }
    }


    /// Gets the [`crate::Timestamp`] that's referenced or returns `None` if the referenced value
    /// isn't a BSON timestamp.
    pub fn as_timestamp(self) -> Option<Timestamp> {
        match self {
            RawBson::Timestamp(timestamp) => Some(timestamp),
            _ => None,
        }
    }

    /// Gets the null value that's referenced or returns `None` if the referenced value isn't a BSON
    /// null.
    pub fn as_null(self) -> Option<()> {
        match self {
            RawBson::Null => Some(()),
            _ => None,
        }
    }

}

/// A visitor used to deserialize types backed by raw BSON.
pub(crate) struct RawBsonVisitor;

impl<'de> Visitor<'de> for RawBsonVisitor {
    type Value = RawBson<'de>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "a raw BSON reference")
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::String(v))
    }

    fn visit_borrowed_bytes<E>(self, bytes: &'de [u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Binary(RawBinary {
            bytes,
            subtype: BinarySubtype::Generic,
        }))
    }

    fn visit_i8<E>(self, v: i8) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Int32(v.into()))
    }

    fn visit_i16<E>(self, v: i16) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Int32(v.into()))
    }

    fn visit_i32<E>(self, v: i32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Int32(v))
    }

    fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Int64(v))
    }

    fn visit_u8<E>(self, value: u8) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        convert_unsigned_to_signed_raw(value.into())
    }

    fn visit_u16<E>(self, value: u16) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        convert_unsigned_to_signed_raw(value.into())
    }

    fn visit_u32<E>(self, value: u32) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        convert_unsigned_to_signed_raw(value.into())
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        convert_unsigned_to_signed_raw(value)
    }

    fn visit_bool<E>(self, v: bool) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Boolean(v))
    }

    fn visit_f64<E>(self, v: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Double(v))
    }

    fn visit_none<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Null)
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(RawBson::Null)
    }

    fn visit_newtype_struct<D>(self, deserializer: D) -> std::result::Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let k = map
            .next_key::<&str>()?
            .ok_or_else(|| serde::de::Error::custom("expected a key when deserializing RawBson"))?;
        match k {
            "$numberDecimalBytes" => {
                let bytes = map.next_value::<ByteBuf>()?;
                return Ok(RawBson::Decimal128(Decimal128::deserialize_from_slice(
                    &bytes,
                )?));
            }
            "$binary" => {
                #[derive(Debug, Deserialize)]
                struct BorrowedBinaryBody<'a> {
                    bytes: &'a [u8],

                    #[serde(rename = "subType")]
                    subtype: u8,
                }

                let v = map.next_value::<BorrowedBinaryBody>()?;

                Ok(RawBson::Binary(RawBinary {
                    bytes: v.bytes,
                    subtype: v.subtype.into(),
                }))
            }
            "$date" => {
                let v = map.next_value::<i64>()?;
                Ok(RawBson::DateTime(DateTime::from_millis(v)))
            }
            "$timestamp" => {
                let v = map.next_value::<extjson::models::TimestampBody>()?;
                Ok(RawBson::Timestamp(Timestamp {
                    time: v.t,
                    increment: v.i,
                }))
            }
            RAW_DOCUMENT_NEWTYPE => {
                let bson = map.next_value::<&[u8]>()?;
                let doc = RawDocument::new(bson).map_err(serde::de::Error::custom)?;
                Ok(RawBson::Document(doc))
            }
            RAW_ARRAY_NEWTYPE => {
                let bson = map.next_value::<&[u8]>()?;
                let doc = RawDocument::new(bson).map_err(serde::de::Error::custom)?;
                Ok(RawBson::Array(RawArray::from_doc(doc)))
            }
            k => Err(serde::de::Error::custom(format!(
                "can't deserialize RawBson from map, key={}",
                k
            ))),
        }
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for RawBson<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_newtype_struct(RAW_BSON_NEWTYPE, RawBsonVisitor)
    }
}

impl<'a> Serialize for RawBson<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            RawBson::Double(v) => serializer.serialize_f64(*v),
            RawBson::String(v) => serializer.serialize_str(v),
            RawBson::Array(v) => v.serialize(serializer),
            RawBson::Document(v) => v.serialize(serializer),
            RawBson::Boolean(v) => serializer.serialize_bool(*v),
            RawBson::Null => serializer.serialize_unit(),
            RawBson::Int32(v) => serializer.serialize_i32(*v),
            RawBson::Int64(v) => serializer.serialize_i64(*v),
            RawBson::UInt32(v) => serializer.serialize_u32(*v),
            RawBson::UInt64(v) => serializer.serialize_u64(*v),
            RawBson::DateTime(dt) => dt.serialize(serializer),
            RawBson::Binary(b) => b.serialize(serializer),
            RawBson::Timestamp(t) => t.serialize(serializer),
            RawBson::Decimal128(d) => d.serialize(serializer),
        }
    }
}

impl<'a> TryFrom<RawBson<'a>> for Bson {
    type Error = Error;

    fn try_from(rawbson: RawBson<'a>) -> Result<Bson> {
        Ok(match rawbson {
            RawBson::Double(d) => Bson::Double(d),
            RawBson::String(s) => Bson::String(s.to_string()),
            RawBson::Document(rawdoc) => {
                let doc = rawdoc.try_into()?;
                Bson::Document(doc)
            }
            RawBson::Array(rawarray) => {
                let mut items = Vec::new();
                for v in rawarray {
                    let bson: Bson = v?.try_into()?;
                    items.push(bson);
                }
                Bson::Array(items)
            }
            RawBson::Binary(rawbson) => {
                let RawBinary {
                    subtype,
                    bytes: data,
                } = rawbson;
                Bson::Binary(crate::Binary {
                    subtype,
                    bytes: data.to_vec(),
                })
            }
            RawBson::Boolean(rawbson) => Bson::Boolean(rawbson),
            RawBson::DateTime(rawbson) => Bson::DateTime(rawbson),
            RawBson::Null => Bson::Null,
            RawBson::Int32(rawbson) => Bson::Int32(rawbson),
            RawBson::Timestamp(rawbson) => Bson::Timestamp(rawbson),
            RawBson::Int64(rawbson) => Bson::Int64(rawbson),
            RawBson::UInt32(rawbson) => Bson::UInt32(rawbson),
            RawBson::UInt64(rawbson) => Bson::UInt64(rawbson),
            RawBson::Decimal128(rawbson) => Bson::Decimal128(rawbson),
        })
    }
}

/// A BSON binary value referencing raw bytes stored elsewhere.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RawBinary<'a> {
    /// The subtype of the binary value.
    pub subtype: BinarySubtype,

    /// The binary bytes.
    pub bytes: &'a [u8],
}

impl<'de: 'a, 'a> Deserialize<'de> for RawBinary<'a> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match RawBson::deserialize(deserializer)? {
            RawBson::Binary(b) => Ok(b),
            c => Err(serde::de::Error::custom(format!(
                "expected binary, but got {:?} instead",
                c
            ))),
        }
    }
}

impl<'a> Serialize for RawBinary<'a> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if let BinarySubtype::Generic = self.subtype {
            serializer.serialize_bytes(self.bytes)
        } else if !serializer.is_human_readable() {
            #[derive(Serialize)]
            struct BorrowedBinary<'a> {
                bytes: &'a Bytes,

                #[serde(rename = "subType")]
                subtype: u8,
            }

            let mut state = serializer.serialize_struct("$binary", 1)?;
            let body = BorrowedBinary {
                bytes: Bytes::new(self.bytes),
                subtype: self.subtype.into(),
            };
            state.serialize_field("$binary", &body)?;
            state.end()
        } else {
            let mut state = serializer.serialize_struct("$binary", 1)?;
            let body = extjson::models::BinaryBody {
                base64: base64::encode(self.bytes),
                subtype: hex::encode([self.subtype.into()]),
            };
            state.serialize_field("$binary", &body)?;
            state.end()
        }
    }
}

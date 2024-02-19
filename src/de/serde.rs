use std::{
    borrow::Cow,
    convert::{TryFrom, TryInto},
    fmt,
    vec,
};

use serde::de::{
    self,
    Deserialize,
    DeserializeSeed,
    Deserializer as _,
    EnumAccess,
    Error,
    MapAccess,
    SeqAccess,
    Unexpected,
    VariantAccess,
    Visitor,
};
use serde_bytes::ByteBuf;

use crate::{
    bson::{Binary, Bson, Timestamp},
    datetime::DateTime,
    document::{Document, IntoIter},
    oid::ObjectId,
    raw::RawBson,
    spec::BinarySubtype,
    uuid::UUID_NEWTYPE_NAME,
    Decimal128,
};

use super::raw::Decimal128Access;

pub(crate) struct BsonVisitor;

struct ObjectIdVisitor;

impl<'de> Visitor<'de> for ObjectIdVisitor {
    type Value = ObjectId;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("expecting an ObjectId")
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        ObjectId::parse_str(value).map_err(|_| {
            E::invalid_value(
                Unexpected::Str(value),
                &"24-character, big-endian hex string",
            )
        })
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let bytes: [u8; 12] = v
            .try_into()
            .map_err(|_| E::invalid_length(v.len(), &"12 bytes"))?;
        Ok(ObjectId::from_bytes(bytes))
    }

    #[inline]
    fn visit_map<V>(self, mut visitor: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        match BsonVisitor.visit_map(&mut visitor)? {
            bson => {
                let err = format!(
                    "expected map containing extended-JSON formatted ObjectId, instead found {}",
                    bson
                );
                Err(de::Error::custom(err))
            }
        }
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if !deserializer.is_human_readable() {
            deserializer.deserialize_bytes(ObjectIdVisitor)
        } else {
            deserializer.deserialize_any(ObjectIdVisitor)
        }
    }
}

impl<'de> Deserialize<'de> for Document {
    /// Deserialize this value given this `Deserializer`.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_map(BsonVisitor).and_then(|bson| {
            if let Bson::Document(doc) = bson {
                Ok(doc)
            } else {
                let err = format!("expected document, found extended JSON data type: {}", bson);
                Err(de::Error::invalid_type(Unexpected::Map, &&err[..]))
            }
        })
    }
}

impl<'de> Deserialize<'de> for Bson {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Bson, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(BsonVisitor)
    }
}

impl<'de> Visitor<'de> for BsonVisitor {
    type Value = Bson;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a Bson")
    }

    #[inline]
    fn visit_bool<E>(self, value: bool) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Boolean(value))
    }

    #[inline]
    fn visit_i8<E>(self, value: i8) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Int32(value as i32))
    }

    #[inline]
    fn visit_u8<E>(self, value: u8) -> Result<Bson, E>
    where
        E: Error,
    {
        convert_unsigned_to_signed(value as u64)
    }

    #[inline]
    fn visit_i16<E>(self, value: i16) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Int32(value as i32))
    }

    #[inline]
    fn visit_u16<E>(self, value: u16) -> Result<Bson, E>
    where
        E: Error,
    {
        convert_unsigned_to_signed(value as u64)
    }

    #[inline]
    fn visit_i32<E>(self, value: i32) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Int32(value))
    }

    #[inline]
    fn visit_u32<E>(self, value: u32) -> Result<Bson, E>
    where
        E: Error,
    {
        convert_unsigned_to_signed(value as u64)
    }

    #[inline]
    fn visit_i64<E>(self, value: i64) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Int64(value))
    }

    #[inline]
    fn visit_u64<E>(self, value: u64) -> Result<Bson, E>
    where
        E: Error,
    {
        convert_unsigned_to_signed(value)
    }

    #[inline]
    fn visit_f64<E>(self, value: f64) -> Result<Bson, E> {
        Ok(Bson::Double(value))
    }

    #[inline]
    fn visit_str<E>(self, value: &str) -> Result<Bson, E>
    where
        E: de::Error,
    {
        self.visit_string(String::from(value))
    }

    #[inline]
    fn visit_string<E>(self, value: String) -> Result<Bson, E> {
        Ok(Bson::String(value))
    }

    #[inline]
    fn visit_none<E>(self) -> Result<Bson, E> {
        Ok(Bson::Null)
    }

    #[inline]
    fn visit_some<D>(self, deserializer: D) -> Result<Bson, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    #[inline]
    fn visit_unit<E>(self) -> Result<Bson, E> {
        Ok(Bson::Null)
    }

    #[inline]
    fn visit_seq<V>(self, mut visitor: V) -> Result<Bson, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut values = Vec::new();

        while let Some(elem) = visitor.next_element()? {
            values.push(elem);
        }

        Ok(Bson::Array(values))
    }

    fn visit_map<V>(self, mut visitor: V) -> Result<Bson, V::Error>
    where
        V: MapAccess<'de>,
    {
        use crate::extjson;

        let mut doc = Document::new();

        while let Some(k) = visitor.next_key::<String>()? {
            match k.as_str() {
                "$numberInt" => {
                    let string: String = visitor.next_value()?;
                    return Ok(Bson::Int32(string.parse().map_err(|_| {
                        V::Error::invalid_value(
                            Unexpected::Str(&string),
                            &"32-bit signed integer as a string",
                        )
                    })?));
                }

                "$numberLong" => {
                    let string: String = visitor.next_value()?;
                    return Ok(Bson::Int64(string.parse().map_err(|_| {
                        V::Error::invalid_value(
                            Unexpected::Str(&string),
                            &"64-bit signed integer as a string",
                        )
                    })?));
                }

                "$numberUInt32" => {
                    let string: String = visitor.next_value()?;
                    return Ok(Bson::UInt32(string.parse().map_err(|_| {
                        V::Error::invalid_value(
                            Unexpected::Str(&string),
                            &"32-bit unsigned integer as a string",
                        )
                    })?));
                }

                "$numberUInt64" => {
                    let string: String = visitor.next_value()?;
                    return Ok(Bson::UInt64(string.parse().map_err(|_| {
                        V::Error::invalid_value(
                            Unexpected::Str(&string),
                            &"64-bit unsigned integer as a string",
                        )
                    })?));
                }

                "$numberDouble" => {
                    let string: String = visitor.next_value()?;
                    let val = match string.as_str() {
                        "Infinity" => Bson::Double(std::f64::INFINITY),
                        "-Infinity" => Bson::Double(std::f64::NEG_INFINITY),
                        "NaN" => Bson::Double(std::f64::NAN),
                        _ => Bson::Int64(string.parse().map_err(|_| {
                            V::Error::invalid_value(
                                Unexpected::Str(&string),
                                &"64-bit signed integer as a string",
                            )
                        })?),
                    };
                    return Ok(val);
                }

                "$binary" => {
                    let v = visitor.next_value::<extjson::models::BinaryBody>()?;
                    return Ok(Bson::Binary(
                        extjson::models::Binary { body: v }
                            .parse()
                            .map_err(Error::custom)?,
                    ));
                }
                "$timestamp" => {
                    let ts = visitor.next_value::<extjson::models::TimestampBody>()?;
                    return Ok(Bson::Timestamp(Timestamp {
                        time: ts.t,
                        increment: ts.i,
                    }));
                }

                "$date" => {
                    let dt = visitor.next_value::<extjson::models::DateTimeBody>()?;
                    return Ok(Bson::DateTime(
                        extjson::models::DateTime { body: dt }
                            .parse()
                            .map_err(Error::custom)?,
                    ));
                }

                "$numberDecimal" => {
                    return Err(Error::custom(
                        "deserializing decimal128 values from strings is not currently supported"
                            .to_string(),
                    ));
                }

                "$numberDecimalBytes" => {
                    let bytes = visitor.next_value::<ByteBuf>()?;
                    return Ok(Bson::Decimal128(Decimal128::deserialize_from_slice(
                        &bytes,
                    )?));
                }

                k => {
                    let v = visitor.next_value::<Bson>()?;
                    doc.insert(k, v);
                }
            }
        }

        Ok(Bson::Document(doc))
    }

    #[inline]
    fn visit_bytes<E>(self, v: &[u8]) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v.to_vec(),
        }))
    }

    #[inline]
    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Bson, E>
    where
        E: Error,
    {
        Ok(Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: v,
        }))
    }

    #[inline]
    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

enum BsonInteger {
    Int32(i32),
    Int64(i64),
}

fn convert_unsigned_to_signed<E>(value: u64) -> Result<Bson, E>
where
    E: Error,
{
    Ok(Bson::UInt64(value))
}

pub(crate) fn convert_unsigned_to_signed_raw<'a, E>(value: u64) -> Result<RawBson<'a>, E>
where
    E: Error,
{
    Ok(RawBson::UInt64(value))
}

/// Serde Deserializer
pub struct Deserializer {
    value: Option<Bson>,
    options: DeserializerOptions,
}

/// Options used to configure a [`Deserializer`]. These can also be passed into
/// [`crate::from_bson_with_options`] and [`crate::from_document_with_options`].
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct DeserializerOptions {
    /// Whether the [`Deserializer`] should present itself as human readable or not.
    /// The default is true.
    pub human_readable: Option<bool>,
}

impl DeserializerOptions {
    /// Create a builder struct used to construct a [`DeserializerOptions`].
    pub fn builder() -> DeserializerOptionsBuilder {
        DeserializerOptionsBuilder {
            options: Default::default(),
        }
    }
}

/// Builder used to construct a [`DeserializerOptions`].
pub struct DeserializerOptionsBuilder {
    options: DeserializerOptions,
}

impl DeserializerOptionsBuilder {
    /// Set the value for [`DeserializerOptions::human_readable`].
    pub fn human_readable(mut self, val: impl Into<Option<bool>>) -> Self {
        self.options.human_readable = val.into();
        self
    }

    /// Consume this builder and produce a [`DeserializerOptions`].
    pub fn build(self) -> DeserializerOptions {
        self.options
    }
}

impl Deserializer {
    /// Construct a new [`Deserializer`] using the default options.
    pub fn new(value: Bson) -> Deserializer {
        Deserializer::new_with_options(value, Default::default())
    }

    /// Create a new [`Deserializer`] using the provided options.
    pub fn new_with_options(value: Bson, options: DeserializerOptions) -> Self {
        Deserializer {
            value: Some(value),
            options,
        }
    }
}

macro_rules! forward_to_deserialize {
    ($(
        $name:ident ( $( $arg:ident : $ty:ty ),* );
    )*) => {
        $(
            forward_to_deserialize!{
                func: $name ( $( $arg: $ty ),* );
            }
        )*
    };

    (func: deserialize_enum ( $( $arg:ident : $ty:ty ),* );) => {
        fn deserialize_enum<V>(
            self,
            $(_: $ty,)*
            _visitor: V,
        ) -> ::std::result::Result<V::Value, Self::Error>
            where V: ::serde::de::Visitor<'de>
        {
            Err(::serde::de::Error::custom("unexpected Enum"))
        }
    };

    (func: $name:ident ( $( $arg:ident : $ty:ty ),* );) => {
        #[inline]
        fn $name<V>(
            self,
            $(_: $ty,)*
            visitor: V,
        ) -> ::std::result::Result<V::Value, Self::Error>
            where V: ::serde::de::Visitor<'de>
        {
            self.deserialize_any(visitor)
        }
    };
}

impl<'de> de::Deserializer<'de> for Deserializer {
    type Error = crate::de::Error;

    fn is_human_readable(&self) -> bool {
        self.options.human_readable.unwrap_or(true)
    }

    #[inline]
    fn deserialize_any<V>(mut self, visitor: V) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = match self.value.take() {
            Some(value) => value,
            None => return Err(crate::de::Error::EndOfStream),
        };

        match value {
            Bson::Double(v) => visitor.visit_f64(v),
            Bson::String(v) => visitor.visit_string(v),
            Bson::Array(v) => {
                let len = v.len();
                visitor.visit_seq(SeqDeserializer {
                    iter: v.into_iter(),
                    options: self.options,
                    len,
                })
            }
            Bson::Document(v) => {
                let len = v.len();
                visitor.visit_map(MapDeserializer {
                    iter: v.into_iter(),
                    value: None,
                    len,
                    options: self.options,
                })
            }
            Bson::Boolean(v) => visitor.visit_bool(v),
            Bson::Null => visitor.visit_unit(),
            Bson::Int32(v) => visitor.visit_i32(v),
            Bson::Int64(v) => visitor.visit_i64(v),
            Bson::UInt32(v) => visitor.visit_u32(v),
            Bson::UInt64(v) => visitor.visit_u64(v),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes,
            }) => visitor.visit_byte_buf(bytes),
            binary @ Bson::Binary(..) => visitor.visit_map(MapDeserializer {
                iter: binary.into_extended_document().into_iter(),
                value: None,
                len: 2,
                options: self.options,
            }),
            Bson::Decimal128(d) => visitor.visit_map(Decimal128Access::new(d)),
            _ => {
                let doc = value.into_extended_document();
                let len = doc.len();
                visitor.visit_map(MapDeserializer {
                    iter: doc.into_iter(),
                    value: None,
                    len,
                    options: self.options,
                })
            }
        }
    }

    #[inline]
    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            _ => self.deserialize_any(visitor),
        }
    }

    #[inline]
    fn deserialize_option<V>(self, visitor: V) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Some(Bson::Null) => visitor.visit_none(),
            Some(_) => visitor.visit_some(self),
            None => Err(crate::de::Error::EndOfStream),
        }
    }

    #[inline]
    fn deserialize_enum<V>(
        mut self,
        _name: &str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        let value = match self.value.take() {
            Some(Bson::Document(value)) => value,
            Some(Bson::String(variant)) => {
                return visitor.visit_enum(EnumDeserializer {
                    val: Bson::String(variant),
                    deserializer: VariantDeserializer {
                        val: None,
                        options: self.options,
                    },
                });
            }
            Some(v) => {
                return Err(crate::de::Error::invalid_type(
                    v.as_unexpected(),
                    &"expected an enum",
                ));
            }
            None => {
                return Err(crate::de::Error::EndOfStream);
            }
        };

        let mut iter = value.into_iter();

        let (variant, value) = match iter.next() {
            Some(v) => v,
            None => {
                return Err(crate::de::Error::invalid_value(
                    Unexpected::Other("empty document"),
                    &"variant name",
                ))
            }
        };

        // enums are encoded in json as maps with a single key:value pair
        match iter.next() {
            Some((k, _)) => Err(crate::de::Error::invalid_value(
                Unexpected::Map,
                &format!("expected map with a single key, got extra key \"{}\"", k).as_str(),
            )),
            None => visitor.visit_enum(EnumDeserializer {
                val: Bson::String(variant),
                deserializer: VariantDeserializer {
                    val: Some(value),
                    options: self.options,
                },
            }),
        }
    }

    #[inline]
    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        // if this is a UUID, ensure that value is a subtype 4 binary
        if name == UUID_NEWTYPE_NAME {
            match self.value {
                Some(Bson::Binary(ref b)) if b.subtype == BinarySubtype::Uuid => {
                    self.deserialize_any(visitor)
                }
                b => Err(Error::custom(format!(
                    "expected Binary with subtype 4, instead got {:?}",
                    b
                ))),
            }
        } else {
            visitor.visit_newtype_struct(self)
        }
    }

    forward_to_deserialize! {
        deserialize_bool();
        deserialize_u8();
        deserialize_u16();
        deserialize_u32();
        deserialize_u64();
        deserialize_i8();
        deserialize_i16();
        deserialize_i32();
        deserialize_i64();
        deserialize_f32();
        deserialize_f64();
        deserialize_char();
        deserialize_str();
        deserialize_string();
        deserialize_unit();
        deserialize_seq();
        deserialize_map();
        deserialize_unit_struct(name: &'static str);
        deserialize_tuple_struct(name: &'static str, len: usize);
        deserialize_struct(name: &'static str, fields: &'static [&'static str]);
        deserialize_tuple(len: usize);
        deserialize_identifier();
        deserialize_ignored_any();
        deserialize_byte_buf();
    }
}

struct EnumDeserializer {
    val: Bson,
    deserializer: VariantDeserializer,
}

impl<'de> EnumAccess<'de> for EnumDeserializer {
    type Error = crate::de::Error;
    type Variant = VariantDeserializer;
    fn variant_seed<V>(self, seed: V) -> crate::de::Result<(V::Value, Self::Variant)>
    where
        V: DeserializeSeed<'de>,
    {
        let dec = Deserializer::new_with_options(self.val, self.deserializer.options.clone());
        let value = seed.deserialize(dec)?;
        Ok((value, self.deserializer))
    }
}

struct VariantDeserializer {
    val: Option<Bson>,
    options: DeserializerOptions,
}

impl<'de> VariantAccess<'de> for VariantDeserializer {
    type Error = crate::de::Error;

    fn unit_variant(mut self) -> crate::de::Result<()> {
        match self.val.take() {
            None => Ok(()),
            Some(val) => {
                Bson::deserialize(Deserializer::new_with_options(val, self.options)).map(|_| ())
            }
        }
    }

    fn newtype_variant_seed<T>(mut self, seed: T) -> crate::de::Result<T::Value>
    where
        T: DeserializeSeed<'de>,
    {
        let dec = Deserializer::new_with_options(
            self.val.take().ok_or(crate::de::Error::EndOfStream)?,
            self.options,
        );
        seed.deserialize(dec)
    }

    fn tuple_variant<V>(mut self, _len: usize, visitor: V) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.val.take().ok_or(crate::de::Error::EndOfStream)? {
            Bson::Array(fields) => {
                let de = SeqDeserializer {
                    len: fields.len(),
                    iter: fields.into_iter(),
                    options: self.options,
                };
                de.deserialize_any(visitor)
            }
            other => Err(crate::de::Error::invalid_type(
                other.as_unexpected(),
                &"expected a tuple",
            )),
        }
    }

    fn struct_variant<V>(
        mut self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        match self.val.take().ok_or(crate::de::Error::EndOfStream)? {
            Bson::Document(fields) => {
                let de = MapDeserializer {
                    len: fields.len(),
                    iter: fields.into_iter(),
                    value: None,
                    options: self.options,
                };
                de.deserialize_any(visitor)
            }
            ref other => Err(crate::de::Error::invalid_type(
                other.as_unexpected(),
                &"expected a struct",
            )),
        }
    }
}

struct SeqDeserializer {
    iter: vec::IntoIter<Bson>,
    len: usize,
    options: DeserializerOptions,
}

impl<'de> de::Deserializer<'de> for SeqDeserializer {
    type Error = crate::de::Error;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        if self.len == 0 {
            visitor.visit_unit()
        } else {
            visitor.visit_seq(self)
        }
    }

    forward_to_deserialize! {
        deserialize_bool();
        deserialize_u8();
        deserialize_u16();
        deserialize_u32();
        deserialize_u64();
        deserialize_i8();
        deserialize_i16();
        deserialize_i32();
        deserialize_i64();
        deserialize_f32();
        deserialize_f64();
        deserialize_char();
        deserialize_str();
        deserialize_string();
        deserialize_unit();
        deserialize_option();
        deserialize_seq();
        deserialize_bytes();
        deserialize_map();
        deserialize_unit_struct(name: &'static str);
        deserialize_newtype_struct(name: &'static str);
        deserialize_tuple_struct(name: &'static str, len: usize);
        deserialize_struct(name: &'static str, fields: &'static [&'static str]);
        deserialize_tuple(len: usize);
        deserialize_enum(name: &'static str, variants: &'static [&'static str]);
        deserialize_identifier();
        deserialize_ignored_any();
        deserialize_byte_buf();
    }
}

impl<'de> SeqAccess<'de> for SeqDeserializer {
    type Error = crate::de::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> crate::de::Result<Option<T::Value>>
    where
        T: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            None => Ok(None),
            Some(value) => {
                self.len -= 1;
                let de = Deserializer::new_with_options(value, self.options.clone());
                match seed.deserialize(de) {
                    Ok(value) => Ok(Some(value)),
                    Err(err) => Err(err),
                }
            }
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

pub(crate) struct MapDeserializer {
    pub(crate) iter: IntoIter,
    pub(crate) value: Option<Bson>,
    pub(crate) len: usize,
    pub(crate) options: DeserializerOptions,
}

impl MapDeserializer {
    pub(crate) fn new(doc: Document) -> Self {
        let len = doc.len();
        MapDeserializer {
            iter: doc.into_iter(),
            len,
            value: None,
            options: Default::default(),
        }
    }
}

impl<'de> MapAccess<'de> for MapDeserializer {
    type Error = crate::de::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> crate::de::Result<Option<K::Value>>
    where
        K: DeserializeSeed<'de>,
    {
        match self.iter.next() {
            Some((key, value)) => {
                self.len -= 1;
                self.value = Some(value);

                let de = Deserializer::new_with_options(Bson::String(key), self.options.clone());
                match seed.deserialize(de) {
                    Ok(val) => Ok(Some(val)),
                    Err(e) => Err(e),
                }
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> crate::de::Result<V::Value>
    where
        V: DeserializeSeed<'de>,
    {
        let value = self.value.take().ok_or(crate::de::Error::EndOfStream)?;
        let de = Deserializer::new_with_options(value, self.options.clone());
        seed.deserialize(de)
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

impl<'de> de::Deserializer<'de> for MapDeserializer {
    type Error = crate::de::Error;

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> crate::de::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    forward_to_deserialize! {
        deserialize_bool();
        deserialize_u8();
        deserialize_u16();
        deserialize_u32();
        deserialize_u64();
        deserialize_i8();
        deserialize_i16();
        deserialize_i32();
        deserialize_i64();
        deserialize_f32();
        deserialize_f64();
        deserialize_char();
        deserialize_str();
        deserialize_string();
        deserialize_unit();
        deserialize_option();
        deserialize_seq();
        deserialize_bytes();
        deserialize_map();
        deserialize_unit_struct(name: &'static str);
        deserialize_newtype_struct(name: &'static str);
        deserialize_tuple_struct(name: &'static str, len: usize);
        deserialize_struct(name: &'static str, fields: &'static [&'static str]);
        deserialize_tuple(len: usize);
        deserialize_enum(name: &'static str, variants: &'static [&'static str]);
        deserialize_identifier();
        deserialize_ignored_any();
        deserialize_byte_buf();
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        match Bson::deserialize(deserializer)? {
            Bson::Timestamp(timestamp) => Ok(timestamp),
            _ => Err(D::Error::custom("expecting Timestamp")),
        }
    }
}


impl<'de> Deserialize<'de> for Binary {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        match Bson::deserialize(deserializer)? {
            Bson::Binary(binary) => Ok(binary),
            d => Err(D::Error::custom(format!(
                "expecting Binary but got {:?} instead",
                d
            ))),
        }
    }
}

impl<'de> Deserialize<'de> for Decimal128 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        match Bson::deserialize(deserializer)? {
            Bson::Decimal128(d128) => Ok(d128),
            o => Err(D::Error::custom(format!(
                "expecting Decimal128, got {:?}",
                o
            ))),
        }
    }
}

impl<'de> Deserialize<'de> for DateTime {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: de::Deserializer<'de>,
    {
        match Bson::deserialize(deserializer)? {
            Bson::DateTime(dt) => Ok(dt),
            _ => Err(D::Error::custom("expecting DateTime")),
        }
    }
}
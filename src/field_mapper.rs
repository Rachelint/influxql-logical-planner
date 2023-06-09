#![allow(dead_code)]

use crate::provider::InfluxColumnType;
use crate::provider::SchemaProvider;
use crate::var_ref::field_type_to_var_ref_data_type;
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use influxql_parser::expression::VarRefDataType;
use std::collections::{HashMap, HashSet};

pub(crate) type FieldTypeMap = HashMap<String, VarRefDataType>;
pub(crate) type TagSet = HashSet<String>;

pub(crate) fn field_and_dimensions(
    s: &dyn SchemaProvider,
    name: &str,
) -> Result<Option<(FieldTypeMap, TagSet)>> {
    let schema = s.table_schema(name).map_err(|e| {
        DataFusionError::Internal(format!(
            "failed to find schema for measurement in field_and_dimensions, measurement:{name}, err:{e}",
        ))
    })?;
    match schema {
        Some(iox) => Ok(Some((
            FieldTypeMap::from_iter(iox.columns().iter().filter_map(
                |(col_type, f)| match col_type {
                    InfluxColumnType::Field(ft) => {
                        Some((f.name().clone(), field_type_to_var_ref_data_type(*ft)))
                    }
                    _ => None,
                },
            )),
            iox.tags()
                .iter()
                .map(|f| f.name().clone())
                .collect::<TagSet>(),
        ))),
        None => Ok(None),
    }
}

pub(crate) fn map_type(
    s: &dyn SchemaProvider,
    measurement_name: &str,
    field: &str,
) -> Result<Option<VarRefDataType>> {
    let schema = s.table_schema(measurement_name).map_err(|e| {
        DataFusionError::Internal(format!(
            "failed to find schema for measurement in map_type, measurement:{measurement_name}, err:{e}",
        ))
    })?;
    match schema {
        Some(iox) => Ok(match iox.find_index_of(field) {
            Some(i) => match iox.column(i).0 {
                InfluxColumnType::Field(ft) => Some(field_type_to_var_ref_data_type(ft)),
                InfluxColumnType::Tag => Some(VarRefDataType::Tag),
                InfluxColumnType::Timestamp => None,
            },
            None => None,
        }),
        None => Ok(None),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::MockSchemaProvider;
    use assert_matches::assert_matches;

    #[test]
    fn test_schema_field_mapper() {
        let namespace = MockSchemaProvider::default();

        // Measurement exists
        let (field_set, tag_set) = field_and_dimensions(&namespace, "cpu").unwrap().unwrap();
        assert_eq!(
            field_set,
            FieldTypeMap::from([
                ("usage_user".to_string(), VarRefDataType::Float),
                ("usage_system".to_string(), VarRefDataType::Float),
                ("usage_idle".to_string(), VarRefDataType::Float),
            ])
        );
        assert_eq!(
            tag_set,
            TagSet::from(["cpu".to_string(), "host".to_string(), "region".to_string()])
        );

        // Measurement does not exist
        assert!(field_and_dimensions(&namespace, "cpu2").unwrap().is_none());

        // `map_type` API calls

        // Returns expected type
        assert_matches!(
            map_type(&namespace, "cpu", "usage_user").unwrap(),
            Some(VarRefDataType::Float)
        );
        assert_matches!(
            map_type(&namespace, "cpu", "host").unwrap(),
            Some(VarRefDataType::Tag)
        );
        // Returns None for nonexistent field
        assert!(map_type(&namespace, "cpu", "nonexistent")
            .unwrap()
            .is_none());
        // Returns None for nonexistent measurement
        assert!(map_type(&namespace, "nonexistent", "usage")
            .unwrap()
            .is_none());
    }
}

//! APIs for testing.
use crate::provider::Schema;
use crate::provider::{InfluxColumnType, InfluxFieldType, SchemaProvider};
use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef, TimeUnit};
use datafusion::common::{DataFusionError, Result};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::TableSource;
use influxql_parser::parse_statements;
use influxql_parser::select::{Field, SelectStatement};
use influxql_parser::statement::Statement;
use itertools::Itertools;
use std::collections::HashMap;

use std::sync::Arc;

const TEST_META_KEY: &str = "test_key";

#[macro_export]
/// Assert that an operation fails with one particular error. Panics if the operation succeeds.
/// Prints debug format of the error value if it doesn't match the specified pattern.
macro_rules! assert_error {
    ($OPERATION: expr, $(|)? $( $ERROR_PATTERN:pat_param )|+ $( if $GUARD: expr )? $(,)?) => {
        let err = $OPERATION.unwrap_err();
        assert!(
            matches!(err, $( $ERROR_PATTERN )|+ $( if $GUARD )?),
            "Expected {}, but got {:?}",
            stringify!($( $ERROR_PATTERN )|+ $( if $GUARD )?),
            err
        );
    };
}

/// Returns the first `Field` of the `SELECT` statement.
pub(crate) fn get_first_field(s: &str) -> Field {
    parse_select(s).fields.head().unwrap().clone()
}

/// Returns the InfluxQL [`SelectStatement`] for the specified SQL, `s`.
pub(crate) fn parse_select(s: &str) -> SelectStatement {
    let statements = parse_statements(s).unwrap();
    match statements.first() {
        Some(Statement::Select(sel)) => *sel.clone(),
        _ => panic!("expected SELECT statement"),
    }
}

/// Module which provides a test database and schema for InfluxQL tests.
pub(crate) mod database {
    use super::*;

    /// Return a set of schemas that make up the test database.
    pub(crate) fn schemas() -> Vec<(String, MockSchema)> {
        vec![
            (
                "cpu".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("host")
                    .tag("region")
                    .tag("cpu")
                    .influx_field("usage_user", InfluxFieldType::Float)
                    .influx_field("usage_system", InfluxFieldType::Float)
                    .influx_field("usage_idle", InfluxFieldType::Float)
                    .build(),
            ),
            (
                "disk".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("host")
                    .tag("region")
                    .tag("device")
                    .influx_field("bytes_used", InfluxFieldType::Integer)
                    .influx_field("bytes_free", InfluxFieldType::Integer)
                    .build(),
            ),
            (
                "diskio".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("host")
                    .tag("region")
                    .tag("status")
                    .influx_field("bytes_read", InfluxFieldType::Integer)
                    .influx_field("bytes_written", InfluxFieldType::Integer)
                    .influx_field("read_utilization", InfluxFieldType::Float)
                    .influx_field("write_utilization", InfluxFieldType::Float)
                    .influx_field("is_local", InfluxFieldType::Boolean)
                    .build(),
            ),
            // Schemas for testing merged schemas
            (
                "temp_01".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("shared_tag0")
                    .tag("shared_tag1")
                    .influx_field("shared_field0", InfluxFieldType::Float)
                    .influx_field("field_f64", InfluxFieldType::Float)
                    .influx_field("field_i64", InfluxFieldType::Integer)
                    .influx_field("field_u64", InfluxFieldType::UInteger)
                    .influx_field("field_str", InfluxFieldType::String)
                    .build(),
            ),
            (
                "temp_02".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("shared_tag0")
                    .tag("shared_tag1")
                    .influx_field("shared_field0", InfluxFieldType::Integer)
                    .build(),
            ),
            (
                "temp_03".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("shared_tag0")
                    .tag("shared_tag1")
                    .influx_field("shared_field0", InfluxFieldType::String)
                    .build(),
            ),
            // Schemas for testing clashing column names when merging across measurements
            (
                "merge_00".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("col0")
                    .influx_field("col1", InfluxFieldType::Float)
                    .influx_field("col2", InfluxFieldType::Boolean)
                    .influx_field("col3", InfluxFieldType::String)
                    .build(),
            ),
            (
                "merge_01".to_string(),
                MockSchemaBuilder::new()
                    .timestamp()
                    .tag("col1")
                    .influx_field("col0", InfluxFieldType::Float)
                    .influx_field("col3", InfluxFieldType::Boolean)
                    .influx_field("col2", InfluxFieldType::String)
                    .build(),
            ),
        ]
    }
}

pub(crate) struct MockSchemaProvider {
    tables: HashMap<String, (Arc<dyn TableSource>, Arc<dyn Schema>)>,
}

impl Default for MockSchemaProvider {
    fn default() -> Self {
        let mut res = Self {
            tables: HashMap::new(),
        };
        res.add_schemas(database::schemas());
        res
    }
}

impl MockSchemaProvider {
    pub(crate) fn add_schema(&mut self, measurement: String, schema: MockSchema) {
        let s = Arc::new(EmptyTable::new(to_arrow_schema(&schema)));
        self.tables
            .insert(measurement, (provider_as_source(s), Arc::new(schema)));
    }

    pub(crate) fn add_schemas(&mut self, schemas: impl IntoIterator<Item = (String, MockSchema)>) {
        schemas
            .into_iter()
            .for_each(|(measurement, schema)| self.add_schema(measurement, schema));
    }
}

impl SchemaProvider for MockSchemaProvider {
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>> {
        self.tables
            .get(name)
            .map(|(t, _)| t.clone())
            .ok_or_else(|| DataFusionError::Plan(format!("measurement does not exist: {name}")))
    }

    fn table_names(&self) -> Result<Vec<&'_ str>> {
        Ok(self
            .tables
            .keys()
            .map(|k| k.as_str())
            .sorted()
            .collect::<Vec<_>>())
    }

    fn table_schema(&self, name: &str) -> Result<Option<Arc<dyn Schema>>> {
        Ok(self.tables.get(name).map(|(_, schema)| schema.clone()))
    }

    fn table_exists(&self, name: &str) -> Result<bool> {
        Ok(self.table_names().unwrap().contains(&name))
    }
}

/// Schema for testing
pub struct MockSchema {
    fields: Vec<(ArrowField, InfluxColumnType)>,
}

impl Schema for MockSchema {
    fn columns(&self) -> Vec<(InfluxColumnType, &ArrowField)> {
        self.fields
            .iter()
            .map(|(field, influx_type)| (*influx_type, field))
            .collect()
    }

    fn tags(&self) -> Vec<&ArrowField> {
        self.fields
            .iter()
            .filter_map(|(field, influx_type)| {
                if matches!(influx_type, InfluxColumnType::Tag) {
                    Some(field)
                } else {
                    None
                }
            })
            .collect()
    }

    fn fields(&self) -> Vec<&ArrowField> {
        self.fields
            .iter()
            .filter_map(|(field, influx_type)| {
                if matches!(influx_type, InfluxColumnType::Field(..)) {
                    Some(field)
                } else {
                    None
                }
            })
            .collect()
    }

    fn time(&self) -> &ArrowField {
        let time_column = self
            .fields
            .iter()
            .find(|(_, influx_type)| matches!(influx_type, InfluxColumnType::Timestamp))
            .unwrap();

        &time_column.0
    }

    fn column(&self, idx: usize) -> (InfluxColumnType, &ArrowField) {
        let (field, influx_type) = &self.fields[idx];

        (*influx_type, field)
    }

    fn find_index_of(&self, name: &str) -> Option<usize> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, (field, _))| field.name() == name)
            .map(|(index, _)| index)
    }
}

/// Builder for a mock schema
#[derive(Debug, Default, Clone)]
pub struct MockSchemaBuilder {
    /// The fields, in order
    fields: Vec<(ArrowField, InfluxColumnType)>,
}

impl MockSchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a new tag column to this schema. By default tags are
    /// potentially nullable as they are not guaranteed to be present
    /// for all rows
    pub fn tag(&mut self, column_name: &str) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Tag;
        let arrow_type = influx_column_type_to_arrow_type(&influxdb_column_type);

        self.add_column(column_name, true, influxdb_column_type, arrow_type)
    }

    /// Add a new field column with the specified InfluxDB data model type
    pub fn influx_field(
        &mut self,
        column_name: &str,
        influxdb_field_type: InfluxFieldType,
    ) -> &mut Self {
        let arrow_type: DataType =
            influx_column_type_to_arrow_type(&InfluxColumnType::Field(influxdb_field_type));
        self.add_column(
            column_name,
            true,
            InfluxColumnType::Field(influxdb_field_type),
            arrow_type,
        )
    }

    /// Add the InfluxDB data model timestamp column
    pub fn timestamp(&mut self) -> &mut Self {
        let influxdb_column_type = InfluxColumnType::Timestamp;
        let arrow_type = influx_column_type_to_arrow_type(&influxdb_column_type);
        self.add_column("time", false, influxdb_column_type, arrow_type)
    }

    pub fn build(&mut self) -> MockSchema {
        self.fields.sort_by(|a, b| a.0.name().cmp(b.0.name()));

        MockSchema {
            fields: self.fields.clone(),
        }
    }

    /// Internal helper method to add a column definition
    fn add_column(
        &mut self,
        column_name: &str,
        nullable: bool,
        column_type: InfluxColumnType,
        arrow_type: DataType,
    ) -> &mut Self {
        let mut field = ArrowField::new(column_name, arrow_type, nullable);
        field.set_metadata(HashMap::from([(
            TEST_META_KEY.to_string(),
            influx_column_type_to_str(&column_type).to_string(),
        )]));

        self.fields.push((field, column_type));
        self
    }
}

fn influx_column_type_to_arrow_type(column_type: &InfluxColumnType) -> DataType {
    match column_type {
        InfluxColumnType::Tag => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        InfluxColumnType::Field(InfluxFieldType::Float) => DataType::Float64,
        InfluxColumnType::Field(InfluxFieldType::Integer) => DataType::Int64,
        InfluxColumnType::Field(InfluxFieldType::UInteger) => DataType::UInt64,
        InfluxColumnType::Field(InfluxFieldType::String) => DataType::Utf8,
        InfluxColumnType::Field(InfluxFieldType::Boolean) => DataType::Boolean,
        InfluxColumnType::Timestamp => DataType::Timestamp(TimeUnit::Nanosecond, None),
    }
}

/// "serialization" to strings that are stored in arrow metadata
fn influx_column_type_to_str(column_type: &InfluxColumnType) -> &'static str {
    match column_type {
        InfluxColumnType::Tag => "iox::column_type::tag",
        InfluxColumnType::Field(InfluxFieldType::Float) => "iox::column_type::field::float",
        InfluxColumnType::Field(InfluxFieldType::Integer) => "iox::column_type::field::integer",
        InfluxColumnType::Field(InfluxFieldType::UInteger) => "iox::column_type::field::uinteger",
        InfluxColumnType::Field(InfluxFieldType::String) => "iox::column_type::field::string",
        InfluxColumnType::Field(InfluxFieldType::Boolean) => "iox::column_type::field::boolean",
        InfluxColumnType::Timestamp => "iox::column_type::timestamp",
    }
}

fn str_to_influx_column_type(column_type_str: &str) -> InfluxColumnType {
    match column_type_str {
        "iox::column_type::tag" => InfluxColumnType::Tag,
        "iox::column_type::field::float" => InfluxColumnType::Field(InfluxFieldType::Float),
        "iox::column_type::field::integer" => InfluxColumnType::Field(InfluxFieldType::Integer),
        "iox::column_type::field::uinteger" => InfluxColumnType::Field(InfluxFieldType::UInteger),
        "iox::column_type::field::string" => InfluxColumnType::Field(InfluxFieldType::String),
        "iox::column_type::field::boolean" => InfluxColumnType::Field(InfluxFieldType::Boolean),
        "iox::column_type::timestamp" => InfluxColumnType::Timestamp,
        _ => unreachable!(),
    }
}

/// Gets the influx type for a field
fn get_influx_type(field: &ArrowField) -> InfluxColumnType {
    let column_type_str = field.metadata().get(TEST_META_KEY).unwrap().as_str();
    str_to_influx_column_type(column_type_str)
}

pub fn to_arrow_schema(schema: &MockSchema) -> SchemaRef {
    let arrow_fields = schema
        .columns()
        .iter()
        .map(|(_, field)| (*field).clone())
        .collect::<Vec<_>>();
    Arc::new(ArrowSchema::new(arrow_fields))
}

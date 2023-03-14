use std::sync::Arc;

use arrow::datatypes::Field;
use datafusion::error::Result;
use datafusion::{common::DFSchemaRef, logical_expr::TableSource};

/// The `SchemaProvider` trait allows the InfluxQL query planner to obtain
/// meta-data about tables referenced in InfluxQL statements.
// TODO: define the error for crate rather than use error of datafusion.
pub trait SchemaProvider {
    fn get_table_provider(&self, name: &str) -> Result<Arc<dyn TableSource>>;

    /// The collection of tables for this schema.
    fn table_names(&self) -> Result<Vec<&'_ str>>;

    /// Test if a table with the specified `name` exists.
    fn table_exists(&self, name: &str) -> Result<bool>;

    /// Get the schema for the specified `table`.
    fn table_schema(&self, name: &str) -> Result<Option<Arc<dyn Schema>>>;
}

pub trait Schema: Send + Sync + 'static {
    /// Returns an iterator of `(Option<InfluxColumnType>, &Field)` for
    /// all the columns of this schema, in order
    fn columns(&self) -> Vec<(InfluxColumnType, &Field)>;

    /// Returns an iterator of `&Field` for all the tag columns of
    /// this schema, in order
    fn tags(&self) -> Vec<&Field>;

    /// Returns an iterator of `&Field` for all the field columns of
    /// this schema, in order
    fn fields(&self) -> Vec<&Field>;

    /// Returns an iterator of `&Field` for all the timestamp columns
    /// of this schema, in order. At the time of writing there should
    /// be only one or 0 such columns
    fn time(&self) -> &Field;

    /// Return the InfluxDB data model type, if any, and underlying arrow   
    /// schema field for the column at index `idx`. Panics if `idx` is
    /// greater than or equal to self.len()
    ///
    /// if there is no corresponding influx metadata,
    /// returns None for the influxdb_column_type
    fn column(&self, idx: usize) -> (InfluxColumnType, &Field);

    /// Find the index of the column with the given name, if any.
    fn find_index_of(&self, name: &str) -> Option<usize>;
}

/// Column types.
///
/// Includes types for tags and fields in the InfluxDB data model, as described in the
/// [documentation](https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/).
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfluxColumnType {
    /// Tag
    ///
    /// Note: tags are always stored as a Utf8, but eventually this
    /// should allow for both Utf8 and Dictionary
    Tag,

    /// Field: Data of type in InfluxDB Data model
    Field(InfluxFieldType),

    /// Timestamp
    ///
    /// 64 bit timestamp "UNIX timestamps" representing nanoseconds
    /// since the UNIX epoch (00:00:00 UTC on 1 January 1970).
    Timestamp,
}

/// Field value types for InfluxDB 2.0 data model, as defined in
/// [the documentation]: <https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/>
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum InfluxFieldType {
    /// 64-bit floating point number (TDB if NULLs / Nans are allowed)
    Float,
    /// 64-bit signed integer
    Integer,
    /// Unsigned 64-bit integers
    UInteger,
    /// UTF-8 encoded string
    String,
    /// true or false
    Boolean,
}

/// Container for both the DataFusion and equivalent IOx schema.
pub(crate) struct Schemas {
    pub(crate) df_schema: DFSchemaRef,
    pub(crate) iox_schema: Arc<dyn Schema>,
}

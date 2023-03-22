use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result, ToDFSchema};
use datafusion::optimizer::alias;
use datafusion::prelude::{max, min, sum};
use datafusion::{
    logical_expr::expr::Sort,
    prelude::{lit, Column, Expr},
    scalar::ScalarValue,
};

pub fn aggr_expr(func: &str, base: Expr) -> Result<Expr> {
    let expr = match func {
        "sum" => sum(base),
        "min" => min(base),
        "max" => max(base),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "Unsupported aggr func, name:{func}"
            )))
        }
    };

    Ok(expr)
}

/// Traits to help creating DataFusion [`Expr`]s
pub trait AsExpr {
    /// Creates a DataFusion expr
    fn as_expr(&self) -> Expr;

    /// creates a DataFusion SortExpr
    fn as_sort_expr(&self) -> Expr {
        Expr::Sort(Sort {
            expr: Box::new(self.as_expr()),
            asc: true, // Sort ASCENDING
            nulls_first: true,
        })
    }
}

impl AsExpr for Arc<str> {
    fn as_expr(&self) -> Expr {
        self.as_ref().as_expr()
    }
}

impl AsExpr for str {
    fn as_expr(&self) -> Expr {
        // note using `col(<ident>)` will parse identifiers and try to
        // split them on `.`.
        //
        // So it would treat 'foo.bar' as table 'foo', column 'bar'
        //
        // This is not correct for influxrpc, so instead treat it
        // like the column "foo.bar"
        Expr::Column(Column {
            relation: None,
            name: self.into(),
        })
    }
}

impl AsExpr for Expr {
    fn as_expr(&self) -> Expr {
        self.clone()
    }
}

/// Creates an `Expr` that represents a Dictionary encoded string (e.g
/// the type of constant that a tag would be compared to)
pub fn lit_dict(value: &str) -> Expr {
    // expr has been type coerced
    // lit(ScalarValue::Dictionary(
    //     Box::new(DataType::Int32),
    //     Box::new(ScalarValue::new_utf8(value)),
    // ))
    lit(ScalarValue::Utf8(Some(value.to_string())))
}

/// Creates expression like:
/// start <= time && time < end
pub fn make_range_expr(start: i64, end: i64, time: impl AsRef<str>) -> Expr {
    // We need to cast the start and end values to timestamps
    // the equivalent of:
    let ts_start = ScalarValue::TimestampNanosecond(Some(start), None);
    let ts_end = ScalarValue::TimestampNanosecond(Some(end), None);

    let time_col = time.as_ref().as_expr();
    let ts_low = lit(ts_start).lt_eq(time_col.clone());
    let ts_high = time_col.lt(lit(ts_end));

    ts_low.and(ts_high)
}

// //! Contains the IOx InfluxQL query planner
// #![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
// #![warn(
//     missing_debug_implementations,
//     clippy::explicit_iter_loop,
//     clippy::use_self,
//     clippy::clone_on_ref_ptr,
//     clippy::future_not_send,
//     clippy::todo,
//     clippy::dbg_macro
// )]

pub(crate) mod datafusion_util;
pub(crate) mod expr_type_evaluator;
pub(crate) mod field;
pub(crate) mod field_mapper;
pub mod planner;
pub(crate) mod planner_rewrite_expression;
pub(crate) mod planner_time_range_expression;
pub mod provider;
pub(crate) mod rewriter;
#[cfg(any(test, feature = "test"))]
pub(crate) mod test_utils;
pub(crate) mod timestamp;
pub(crate) mod util;
pub(crate) mod var_ref;

pub use datafusion::error::{DataFusionError, Result};

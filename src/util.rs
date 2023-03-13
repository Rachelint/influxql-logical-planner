use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use influxql_parser::expression::BinaryOperator;
use influxql_parser::string::Regex;

pub(crate) fn binary_operator_to_df_operator(op: BinaryOperator) -> Operator {
    match op {
        BinaryOperator::Add => Operator::Plus,
        BinaryOperator::Sub => Operator::Minus,
        BinaryOperator::Mul => Operator::Multiply,
        BinaryOperator::Div => Operator::Divide,
        BinaryOperator::Mod => Operator::Modulo,
        BinaryOperator::BitwiseAnd => Operator::BitwiseAnd,
        BinaryOperator::BitwiseOr => Operator::BitwiseOr,
        BinaryOperator::BitwiseXor => Operator::BitwiseXor,
    }
}

/// Sanitize an InfluxQL regular expression and create a compiled [`regex::Regex`].
pub(crate) fn parse_regex(re: &Regex) -> Result<regex::Regex> {
    let pattern = clean_non_meta_escapes(re.as_str());
    regex::Regex::new(&pattern).map_err(|e| {
        DataFusionError::External(format!("invalid regular expression '{re}': {e}").into())
    })
}

// Copy from influxdb_iox:
// https://github.com/influxdata/influxdb_iox/blob/e7369449f8975f6f86bc665ea3e1f556c2777145/query_functions/src/regex.rs#L147
pub fn clean_non_meta_escapes(pattern: &str) -> String {
    if pattern.is_empty() {
        return pattern.to_string();
    }

    #[derive(Debug, Copy, Clone)]
    enum SlashState {
        No,
        Single,
        Double,
    }

    let mut next_state = SlashState::No;

    let next_chars = pattern
        .chars()
        .map(Some)
        .skip(1)
        .chain(std::iter::once(None));

    // emit char based on previous
    let new_pattern: String = pattern
        .chars()
        .zip(next_chars)
        .filter_map(|(c, next_char)| {
            let cur_state = next_state;
            next_state = match (c, cur_state) {
                ('\\', SlashState::No) => SlashState::Single,
                ('\\', SlashState::Single) => SlashState::Double,
                ('\\', SlashState::Double) => SlashState::Single,
                _ => SlashState::No,
            };

            // Decide to emit `c` or not
            match (cur_state, c, next_char) {
                (SlashState::No, '\\', Some(next_char))
                | (SlashState::Double, '\\', Some(next_char))
                    if !is_valid_character_after_escape(next_char) =>
                {
                    None
                }
                _ => Some(c),
            }
        })
        .collect();

    new_pattern
}

// Copy from influxdb_iox:
// https://github.com/influxdata/influxdb_iox/blob/e7369449f8975f6f86bc665ea3e1f556c2777145/query_functions/src/regex.rs#L123
fn is_valid_character_after_escape(c: char) -> bool {
    // same list as https://docs.rs/regex-syntax/0.6.25/src/regex_syntax/ast/parse.rs.html#1445-1538
    match c {
        '0'..='7' => true,
        '8'..='9' => true,
        'x' | 'u' | 'U' => true,
        'p' | 'P' => true,
        'd' | 's' | 'w' | 'D' | 'S' | 'W' => true,
        _ => regex_syntax::is_meta_character(c),
    }
}

mod test {
    // Copy from influxdb_iox:
    // https://github.com/influxdata/influxdb_iox/blob/e7369449f8975f6f86bc665ea3e1f556c2777145/query_functions/src/regex.rs#L357
    #[test]
    fn test_clean_non_meta_escapes() {
        let cases = vec![
            ("", ""),
            (r#"\"#, r#"\"#),
            (r#"\\"#, r#"\\"#),
            // : is not a special meta character
            (r#"\:"#, r#":"#),
            // . is a special meta character
            (r#"\."#, r#"\."#),
            (r#"foo\"#, r#"foo\"#),
            (r#"foo\\"#, r#"foo\\"#),
            (r#"foo\:"#, r#"foo:"#),
            (r#"foo\xff"#, r#"foo\xff"#),
            (r#"fo\\o"#, r#"fo\\o"#),
            (r#"fo\:o"#, r#"fo:o"#),
            (r#"fo\:o\x123"#, r#"fo:o\x123"#),
            (r#"fo\:o\x123\:"#, r#"fo:o\x123:"#),
            (r#"foo\\\:bar"#, r#"foo\\:bar"#),
            (r#"foo\\\:bar\\\:"#, r#"foo\\:bar\\:"#),
            ("foo", "foo"),
        ];

        for (pattern, expected) in cases {
            let cleaned_pattern = crate::util::clean_non_meta_escapes(pattern);
            assert_eq!(
                cleaned_pattern, expected,
                "Expected '{pattern}' to be cleaned to '{expected}', got '{cleaned_pattern}'"
            );
        }
    }
}

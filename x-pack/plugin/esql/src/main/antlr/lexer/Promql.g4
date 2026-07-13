/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Promql;

//
// PromQL command with optional parameters and query text
//
PROMQL : 'promql' -> pushMode(PROMQL_MODE);

mode PROMQL_MODE;

// Used for for parameter names
// Note that this overlaps with UNQUOTED_SOURCE which is needed for index patterns.
// For parameter names we restrict to letters, digits, and underscores only - to avoid mistaking PromQL queries for parameter names.
// Example: PROMQL step=1m foo{bar="baz"} - foo{bar should not be treated as a parameter name.
// As UNQUOTED_SOURCE supports a superset of UNQUOTED_IDENTIFIER characters, we need to define UNQUOTED_IDENTIFIER before UNQUOTED_SOURCE.
// This way, the lexer will match UNQUOTED_IDENTIFIER first when possible.
// This requires us to duplicate the parsing for promql index patterns as we may get an UNQUOTED_IDENTIFIER token instead of UNQUOTED_SOURCE.
PROMQL_UNQUOTED_IDENTIFIER : UNQUOTED_IDENTIFIER  -> type(UNQUOTED_IDENTIFIER);

// Also support quoted identifiers and named parameters
PROMQL_QUOTED_IDENTIFIER: QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
PROMQL_ASSIGN: ASSIGN -> type(ASSIGN);
PROMQL_NAMED_PARAMS: NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);

// indexPattern tokens
PROMQL_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
PROMQL_QUOTED_STRING: QUOTED_STRING -> type(QUOTED_STRING);
PROMQL_COLON : COLON -> type(COLON);
PROMQL_CAST_OP : CAST_OP -> type(CAST_OP);
PROMQL_COMMA : COMMA -> type(COMMA);

// Exit back to default mode on pipe
PROMQL_PARAMS_PIPE : PIPE -> type(PIPE), popMode;

// Opening paren starts query text capture
PROMQL_LP : LP {this.incPromqlDepth();} -> type(LP);
PROMQL_NESTED_RP
    : ')' {this.isPromqlQuery()}? {this.decPromqlDepth();} -> type(RP)
    ;

PROMQL_QUERY_RP
    : ')' {!this.isPromqlQuery()}? {this.resetPromqlDepth();} -> type(RP), popMode
    ;

// Comments and whitespace
PROMQL_PARAMS_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
PROMQL_PARAMS_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
PROMQL_PARAMS_WS : WS -> channel(HIDDEN);

// PromQL-style comments (#)
PROMQL_QUERY_COMMENT
    : '#' ~[\r\n]* '\r'? '\n'?
    ;

PROMQL_SINGLE_QUOTED_STRING
    : '\'' ( '\\' . | ~[\\'] )* '\'';

// catch-all lexer rule to capture other query content
// must be after more specific rules
// and only match single characters to avoid matching more than a specific rule as it would prevent that rule from matching
PROMQL_OTHER_QUERY_CONTENT
    : ~[|()"'`#\r\n ]
    ;

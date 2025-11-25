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
DEV_PROMQL : {this.isDevVersion()}? 'promql' -> pushMode(PROMQL_PARAMS_MODE);

mode PROMQL_PARAMS_MODE;

// Simple unquoted identifier for parameter names and values
PROMQL_UNQUOTED_IDENTIFIER
    : [a-z0-9][a-z0-9_]*           // Starts with letter/digit, followed by letters/digits/underscores
    | [_@][a-z0-9_]+               // OR starts with _/@ followed by at least one alphanumeric/underscore
    ;

// Also support quoted identifiers and named parameters
PROMQL_QUOTED_IDENTIFIER: QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
PROMQL_NAMED_PARAMS: NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);

PROMQL_QUOTED_STRING: QUOTED_STRING -> type(QUOTED_STRING);

// Exit back to default mode on pipe
PROMQL_PARAMS_PIPE : PIPE -> type(PIPE), popMode;

// Opening paren starts query text capture
PROMQL_LP : LP {this.incPromqlDepth();} -> type(LP), pushMode(PROMQL_QUERY_MODE);

// Comments and whitespace
PROMQL_PARAMS_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
PROMQL_PARAMS_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
PROMQL_PARAMS_WS : WS -> channel(HIDDEN);

mode PROMQL_QUERY_MODE;

// Nested opening parens - increment depth and emit LP token
PROMQL_NESTED_LP
    : '(' {this.incPromqlDepth();} -> type(LP)
    ;

// Query text - everything except parens and special characters
PROMQL_QUERY_TEXT
    : ( PROMQL_STRING_LITERAL | PROMQL_QUERY_COMMENT | ~[|()"'`#\r\n] )+    // Exclude both ( and ) from text
    ;

// String literals (preserved with quotes as part of text)
fragment PROMQL_STRING_LITERAL
    : '"'  ( '\\' . | ~[\\"] )* '"'
    | '\'' ( '\\' . | ~[\\'] )* '\''
    | '`'  ~'`'* '`'
    ;

// PromQL-style comments (#)
fragment PROMQL_QUERY_COMMENT
    : '#' ~[\r\n]* '\r'? '\n'?
    ;

PROMQL_NESTED_RP
    : ')' {this.isPromqlQuery()}? {this.decPromqlDepth();} -> type(RP)
    ;

PROMQL_QUERY_RP
    : ')' {!this.isPromqlQuery()}? {this.resetPromqlDepth();} -> type(RP), popMode, popMode
    ;

// Pipe exits both modes
PROMQL_QUERY_PIPE : PIPE -> type(PIPE), popMode, popMode;

// ESQL-style comments
PROMQL_QUERY_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
PROMQL_QUERY_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
PROMQL_QUERY_WS : WS -> channel(HIDDEN);

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Promql;

//
// PromQL command - captures PromQL expression text for delegation to PromqlParser
//
DEV_PROMQL : {this.isDevVersion()}? 'promql' -> pushMode(PROMQL_MODE);

mode PROMQL_MODE;

// Comments go to HIDDEN channel - PromQL uses # for line comments
PROMQL_COMMENT
    : '#' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

// Consolidated token: captures strings and expressions together
// Strings can contain | and other special characters
PROMQL_TEXT
    : ( PROMQL_STRING_LITERAL | ~[|"'`#] )+
    ;

// String literals as fragments (used within PROMQL_TEXT)
fragment PROMQL_STRING_LITERAL
    : '"'  ( '\\' . | ~[\\"] )* '"'
    | '\'' ( '\\' . | ~[\\'] )* '\''
    | '`'  ~'`'* '`'
    ;

// Exit the mode when we encounter a pipe (not inside a string or comment)
PROMQL_PIPE : PIPE -> type(PIPE), popMode;

// Whitespace
PROMQL_WS : WS -> channel(HIDDEN);

// ESQL-style comments for completeness
PROMQL_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
PROMQL_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);

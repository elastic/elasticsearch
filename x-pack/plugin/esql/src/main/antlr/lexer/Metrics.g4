/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Metrics;

//
// METRICS command
//
DEV_METRICS :     {this.isDevVersion()}? 'metrics'       -> pushMode(METRICS_MODE);

mode METRICS_MODE;
METRICS_PIPE : PIPE -> type(PIPE), popMode;

METRICS_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE), popMode, pushMode(CLOSING_METRICS_MODE);
METRICS_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING), popMode, pushMode(CLOSING_METRICS_MODE);

METRICS_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

METRICS_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

METRICS_WS
    : WS -> channel(HIDDEN)
    ;


// TODO: remove this workaround mode - see https://github.com/elastic/elasticsearch/issues/108528
mode CLOSING_METRICS_MODE;

CLOSING_METRICS_COLON
    : COLON -> type(COLON), popMode, pushMode(METRICS_MODE)
    ;

CLOSING_METRICS_COMMA
    : COMMA -> type(COMMA), popMode, pushMode(METRICS_MODE)
    ;

CLOSING_METRICS_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

CLOSING_METRICS_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

CLOSING_METRICS_WS
    : WS -> channel(HIDDEN)
    ;

CLOSING_METRICS_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> popMode, pushMode(EXPRESSION_MODE), type(QUOTED_IDENTIFIER)
    ;

CLOSING_METRICS_UNQUOTED_IDENTIFIER
    :UNQUOTED_IDENTIFIER -> popMode, pushMode(EXPRESSION_MODE), type(UNQUOTED_IDENTIFIER)
    ;

CLOSING_METRICS_BY
    :BY -> popMode, pushMode(EXPRESSION_MODE), type(BY)
    ;

CLOSING_METRICS_PIPE
    : PIPE -> type(PIPE), popMode
    ;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar LoadResult;

//
// LOAD_RESULT command
//
LOAD_RESULT : 'load_result' -> pushMode(LOAD_RESULT_MODE);

mode LOAD_RESULT_MODE;
LOAD_RESULT_PIPE : PIPE -> type(PIPE), popMode;

LOAD_RESULT_UNQUOTED_ID
    : ~["|, \t\r\n]+
    ;

LOAD_RESULT_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);

LOAD_RESULT_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOAD_RESULT_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOAD_RESULT_WS
    : WS -> channel(HIDDEN)
    ;

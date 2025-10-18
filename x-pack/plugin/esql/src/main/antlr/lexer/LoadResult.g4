/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar LoadResult;

//
// LOAD_RESULT command (tech preview - dev/snapshot only)
//
DEV_LOAD_RESULT : {this.isDevVersion()}? 'load_result' -> pushMode(LOAD_RESULT_MODE);

mode LOAD_RESULT_MODE;
LOAD_RESULT_PIPE : PIPE -> type(PIPE), popMode;

// allow quoted string IDs in this mode
LOAD_RESULT_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);

// also allow unquoted ids composed of letters, digits, underscore (common for base64url-like ids)
fragment LR_LETTER : [a-z];
fragment LR_DIGIT : [0-9];
fragment LR_UNDERSCORE : '_';
fragment LR_DASH : '-';
fragment LR_TILDE : '~';
fragment LR_DOT : '.';
fragment LR_UNQUOTED_ID_BODY : (LR_LETTER | LR_DIGIT | LR_UNDERSCORE | LR_DASH | LR_TILDE | LR_DOT)+;
LOAD_RESULT_UNQUOTED_ID : LR_UNQUOTED_ID_BODY -> type(UNQUOTED_IDENTIFIER);

LOAD_RESULT_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOAD_RESULT_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOAD_RESULT_WS
    : WS -> channel(HIDDEN)
    ;



/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Lookup;

//
// LOOKUP ON key
//
DEV_LOOKUP :      {this.isDevVersion()}? 'lookup_🐔'      -> pushMode(LOOKUP_MODE);

mode LOOKUP_MODE;
LOOKUP_PIPE : PIPE -> type(PIPE), popMode;
LOOKUP_COLON : COLON -> type(COLON);
LOOKUP_COMMA : COMMA -> type(COMMA);
LOOKUP_DOT: DOT -> type(DOT);
LOOKUP_ON : ON -> type(ON), pushMode(LOOKUP_FIELD_MODE);

LOOKUP_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
LOOKUP_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);

LOOKUP_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_WS
    : WS -> channel(HIDDEN)
    ;

mode LOOKUP_FIELD_MODE;
LOOKUP_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
LOOKUP_FIELD_COMMA : COMMA -> type(COMMA);
LOOKUP_FIELD_DOT: DOT -> type(DOT);

LOOKUP_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

LOOKUP_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

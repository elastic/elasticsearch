/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Untable;

//
// | UNTABLE nameColumn FOR valueColumn IN (fields*)
//
DEV_UNTABLE :     {this.isDevVersion()}? 'untable'       -> pushMode(UNTABLE_MODE);

mode UNTABLE_MODE;
UNTABLE_PIPE : PIPE -> type(PIPE), popMode;
UNTABLE_COLON : COLON -> type(COLON);
UNTABLE_COMMA : COMMA -> type(COMMA);
UNTABLE_DOT: DOT -> type(DOT);
UNTABLE_ON : IN -> type(IN), pushMode(UNTABLE_FIELD_MODE);
UNTABLE_FOR : FOR -> type(FOR);

UNTABLE_ID_PATTERN : ID_PATTERN -> type(ID_PATTERN);
UNTABLE_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
UNTABLE_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);

UNTABLE_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

UNTABLE_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

UNTABLE_WS
    : WS -> channel(HIDDEN)
    ;

mode UNTABLE_FIELD_MODE;
UNTABLE_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
UNTABLE_FIELD_COMMA : COMMA -> type(COMMA);
UNTABLE_FIELD_DOT: DOT -> type(DOT);
UNTABLE_FIELD_LP : LP -> type(LP);
UNTABLE_FIELD_RP : RP -> type(RP);

UNTABLE_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

UNTABLE_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

UNTABLE_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

UNTABLE_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

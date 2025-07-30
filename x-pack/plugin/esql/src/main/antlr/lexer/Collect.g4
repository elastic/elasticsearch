/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Collect;

//
// | COLLECT command
//
COLLECT : 'collect' -> pushMode(COLLECT_MODE);

mode COLLECT_MODE;
COLLECT_PIPE : PIPE -> type(PIPE), popMode;
// explicit popMode of RP to allow CHANGE_POINT in FORK branches
COLLECT_RP : RP -> type(RP), popMode, popMode;

COLLECT_ID : 'id' -> pushMode(COLLECT_ID_MODE);
COLLECT_INDEX : ~[\\/?"<>| ,#\t\r\n:()]+;

COLLECT_LINE_COMMENT: LINE_COMMENT -> channel(HIDDEN);
COLLECT_MULTILINE_COMMENT: MULTILINE_COMMENT -> channel(HIDDEN);
COLLECT_WS: WS -> channel(HIDDEN);


mode COLLECT_ID_MODE;
COLLECT_ID_PIPE : PIPE -> type(PIPE), popMode, popMode;
COLLECT_ID_RP : RP -> type(RP), popMode, popMode, popMode;
COLLECT_ID_COMMA : COMMA -> type(COMMA);
COLLECT_ID_DOT: DOT -> type(DOT);

COLLECT_ID_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

COLLECT_ID_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

COLLECT_ID_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

COLLECT_ID_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

COLLECT_ID_WS
    : WS -> channel(HIDDEN)
    ;

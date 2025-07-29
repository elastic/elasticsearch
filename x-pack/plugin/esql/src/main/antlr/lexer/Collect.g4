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

COLLECT_INDEX
    : ~[\\/?"<>| ,#\t\r\n:()]+;

COLLECT_LINE_COMMENT: LINE_COMMENT -> channel(HIDDEN);
COLLECT_MULTILINE_COMMENT: MULTILINE_COMMENT -> channel(HIDDEN);
COLLECT_WS: WS -> channel(HIDDEN);

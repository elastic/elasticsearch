/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Show;

//
// SHOW commands
//
SHOW : 'show'                 -> pushMode(SHOW_MODE);


mode SHOW_MODE;
SHOW_PIPE : PIPE -> type(PIPE), popMode;

INFO : 'info';

SHOW_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_WS
    : WS -> channel(HIDDEN)
    ;


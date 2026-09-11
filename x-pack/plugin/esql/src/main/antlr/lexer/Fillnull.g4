/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Fillnull;

//
// FILLNULL <value> ON <fields>
//
// The value is lexed in a dedicated mode (so DEFAULT is a keyword only here) and ON switches to
// PROJECT_MODE so the field list can use the same wildcard patterns as KEEP / DROP. Popping FILLNULL_MODE
// and pushing PROJECT_MODE leaves the exact mode stack KEEP / DROP produce, so PROJECT_PIPE / PROJECT_RP
// (the latter needed for FORK branches) behave identically.
//
DEV_FILLNULL : {this.isDevVersion()}? 'fillnull' -> pushMode(FILLNULL_MODE);

mode FILLNULL_MODE;
FILLNULL_PIPE : PIPE -> type(PIPE), popMode;
// explicit popMode of RP to allow fillnull in FORK branches
FILLNULL_RP : RP -> type(RP), popMode, popMode;

FILLNULL_ON : ON -> type(ON), popMode, pushMode(PROJECT_MODE);

DEFAULT : 'default';

FILLNULL_NULL : NULL -> type(NULL);
FILLNULL_TRUE : TRUE -> type(TRUE);
FILLNULL_FALSE : FALSE -> type(FALSE);

FILLNULL_PLUS : PLUS -> type(PLUS);
FILLNULL_MINUS : MINUS -> type(MINUS);
FILLNULL_DECIMAL_LITERAL : DECIMAL_LITERAL -> type(DECIMAL_LITERAL);
FILLNULL_INTEGER_LITERAL : INTEGER_LITERAL -> type(INTEGER_LITERAL);
FILLNULL_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);
FILLNULL_PARAM : PARAM -> type(PARAM);
FILLNULL_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);

FILLNULL_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

FILLNULL_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

FILLNULL_WS
    : WS -> channel(HIDDEN)
    ;

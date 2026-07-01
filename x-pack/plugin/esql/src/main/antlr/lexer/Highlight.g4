/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Highlight;

//
// HIGHLIGHT [query_string] [ON field, ...] [WITH options]
//
DEV_HIGHLIGHT : {this.isDevVersion()}? 'highlight' -> pushMode(HIGHLIGHT_MODE);

mode HIGHLIGHT_MODE;

HIGHLIGHT_PIPE : PIPE -> type(PIPE), popMode;
// explicit popMode of RP to allow HIGHLIGHT in FORK branches
HIGHLIGHT_RP : RP -> type(RP), popMode, popMode;

HIGHLIGHT_ON : ON -> type(ON);
HIGHLIGHT_WITH : WITH -> type(WITH), popMode, pushMode(EXPRESSION_MODE);

HIGHLIGHT_ASSIGN : ASSIGN -> type(ASSIGN);
HIGHLIGHT_COMMA : COMMA -> type(COMMA);
HIGHLIGHT_DOT : DOT -> type(DOT);

HIGHLIGHT_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);
HIGHLIGHT_QUOTED_IDENTIFIER : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
HIGHLIGHT_UNQUOTED_IDENTIFIER : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
HIGHLIGHT_PARAM : PARAM -> type(PARAM);
HIGHLIGHT_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
HIGHLIGHT_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
HIGHLIGHT_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

HIGHLIGHT_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
HIGHLIGHT_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
HIGHLIGHT_WS : WS -> channel(HIDDEN);

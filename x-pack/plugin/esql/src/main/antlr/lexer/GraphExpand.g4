/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar GraphExpand;

//
// GRAPH EXPAND — snapshot/dev only. Shape follows Explain (gate) + MMR (mode).
// GRAPH_EXPAND_MODE reads the index pattern (UNQUOTED_SOURCE). ON switches to
// GRAPH_FIELD_MODE for seed/match/TO fields so identifiers are not stolen by
// the index-pattern token (same split LOOKUP uses). WHERE / STATS / SORT /
// UNTIL / WITH then pop into EXPRESSION_MODE for their payloads.
//
DEV_GRAPH : {this.isDevVersion()}? 'graph' -> pushMode(GRAPH_EXPAND_MODE);

mode GRAPH_EXPAND_MODE;

GRAPH_PIPE : PIPE -> type(PIPE), popMode;
GRAPH_RP : ')' -> type(RP), popMode, popMode;

EXPAND : 'expand';

GRAPH_ON : ON -> type(ON), pushMode(GRAPH_FIELD_MODE);

GRAPH_COLON : COLON -> type(COLON);
GRAPH_CAST_OP : CAST_OP -> type(CAST_OP);
GRAPH_COMMA : COMMA -> type(COMMA);

GRAPH_UNQUOTED_SOURCE : UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
GRAPH_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);
GRAPH_PARAM : PARAM -> type(PARAM);
GRAPH_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);

GRAPH_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

GRAPH_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

GRAPH_WS
    : WS -> channel(HIDDEN)
    ;

mode GRAPH_FIELD_MODE;

GRAPH_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
// Double push/pop on parens so TO (a, b) balances; FORK's closing ')' still
// double-pops the field mode and the DEFAULT_MODE FORK_LP pushed.
GRAPH_FIELD_LP : '(' -> type(LP), pushMode(GRAPH_FIELD_MODE), pushMode(GRAPH_FIELD_MODE);
GRAPH_FIELD_RP : ')' -> type(RP), popMode, popMode;

TO : 'to';
UNTIL : 'until' -> popMode, popMode, pushMode(EXPRESSION_MODE);

GRAPH_FIELD_EQ : EQ -> type(EQ);
GRAPH_FIELD_WHERE : WHERE -> type(WHERE), popMode, popMode, pushMode(EXPRESSION_MODE);
GRAPH_FIELD_STATS : STATS -> type(STATS), popMode, popMode, pushMode(EXPRESSION_MODE);
GRAPH_FIELD_SORT : SORT -> type(SORT), popMode, popMode, pushMode(EXPRESSION_MODE);
GRAPH_FIELD_WITH : WITH -> type(WITH), popMode, popMode, pushMode(EXPRESSION_MODE);

GRAPH_FIELD_COMMA : COMMA -> type(COMMA);
GRAPH_FIELD_DOT : DOT -> type(DOT);
GRAPH_FIELD_PARAM : PARAM -> type(PARAM);
GRAPH_FIELD_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
GRAPH_FIELD_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
GRAPH_FIELD_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

GRAPH_FIELD_QUOTED_IDENTIFIER : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
GRAPH_FIELD_UNQUOTED_IDENTIFIER : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
GRAPH_FIELD_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET);
GRAPH_FIELD_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET);

GRAPH_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

GRAPH_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

GRAPH_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

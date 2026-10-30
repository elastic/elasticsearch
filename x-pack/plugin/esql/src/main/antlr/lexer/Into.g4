/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Into;

//
// INTO command
//
// INTO names a write target, so it pushes a mode that lexes an index pattern
// the way JOIN does and pops back out on the pipe. Options come in a WITH map after the target, as for RERANK and
// COMPLETION, so WITH leaves this mode for the expression mode, which lexes the map.
//
// Gated by the esql_into feature flag through its capability, like EXTERNAL.
//
DEV_INTO : 'into' {EsqlCapabilities.Cap.INTO_COMMAND.isEnabled()}? -> pushMode(INTO_MODE);

mode INTO_MODE;
INTO_PIPE : PIPE -> type(PIPE), popMode;

INTO_WITH : WITH -> type(WITH), popMode, pushMode(EXPRESSION_MODE);

INTO_UNQUOTED_SOURCE : UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
INTO_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);
INTO_COLON : COLON -> type(COLON);
INTO_SELECTOR : CAST_OP -> type(CAST_OP);
// A closing bracket ends a subquery or FORK branch as well as the command, so both modes are popped: the parser
// then sees the misplaced INTO and refuses it.
INTO_RP : RP -> type(RP), popMode, popMode;

INTO_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

INTO_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

INTO_WS
    : WS -> channel(HIDDEN)
    ;

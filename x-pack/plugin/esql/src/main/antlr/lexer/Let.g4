/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Let;

//
// LET name = (subquery);
//
DEV_LET : {this.isDevVersion()}? 'let' -> pushMode(LET_MODE);

mode LET_MODE;
// Commands inside a binding subquery need to break out of their mode and
// the DEFAULT_MODE pushed by LET_LP when they encounter RP — that is why
// every command mode's RP does `popMode, popMode`. We must push DEFAULT_MODE
// here (not replace via `mode(DEFAULT_MODE)`) so that the second popMode of
// the inner command's RP unwinds back to LET_MODE rather than past it.
LET_LP        : LP        -> type(LP), pushMode(DEFAULT_MODE);
// Explicit double popMode to allow FORK inside a LET binding subquery: when
// a FORK branch is the last content of the binding, the closing RP is seen
// in FORK_MODE, and FORK_RP does double popMode. The analogous token here
// handles a stray RP seen directly in LET_MODE (syntax error path).
LET_RP        : RP        -> type(RP), popMode, popMode;
LET_SEMICOLON : SEMICOLON -> type(SEMICOLON), popMode;
LET_ASSIGN    : ASSIGN    -> type(ASSIGN);

LET_UNQUOTED_IDENTIFIER : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
LET_QUOTED_IDENTIFIER   : QUOTED_IDENTIFIER   -> type(QUOTED_IDENTIFIER);

LET_LINE_COMMENT      : LINE_COMMENT      -> channel(HIDDEN);
LET_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);
LET_WS                : WS                -> channel(HIDDEN);

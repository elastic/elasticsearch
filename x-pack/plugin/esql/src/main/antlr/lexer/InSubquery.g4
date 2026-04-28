/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar InSubquery;

fragment IN_EXPRESSION_INIT : [a-z];

// Entered IN_SUBQUERY preemptively after the IN keyword
//
// If we find a source command (FROM, SHOW, TS, etc) after the IN, we are indeed in a
// subquery and we can enter DEFAULT MODE to parse that subquery
//
// Otherwise we jump back to EXPRESSION_MODE because we are in a IN(expr1, ..., exprN) case
mode IN_SUBQUERY;

AFTER_IN_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

AFTER_IN_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

AFTER_IN_WS
    : WS -> channel(HIDDEN)
    ;

// Match `( keyword WS*` as lookahead, but emit only `(` as LP and rewind so
// the keyword is re-lexed in DEFAULT_MODE. `mode(DEFAULT_MODE)` replaces
// (rather than pushes) so the stack depth matches the EXPRESSION_MODE →
// DEFAULT_MODE pairing the existing FROM_RP / PROJECT_RP / etc. (each
// `popMode, popMode`) expect when closing the subquery.
IN_SUBQUERY_LP
    : {this.isDevVersion()}?
      '(' ('from' | 'row' | 'show' | 'ts' | 'promql') WS*
      { this.rewindToTokenStart(1); }
      -> type(LP), mode(DEFAULT_MODE)
    ;

// Match a single char as lookahead, then fully unwind so EXPRESSION_MODE
// re-lexes from the original position.
IN_EXPR_FALLBACK
    : . { this.rewindToTokenStart(0); } -> popMode, skip
    ;

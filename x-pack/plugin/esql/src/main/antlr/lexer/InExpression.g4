/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar InExpression;

fragment IN_EXPRESSION_INIT : [a-z];

// Entered IN_MODE after the IN keyword
//
// - If we find a source command (FROM, SHOW, TS, etc) after the IN, we are in a
// subquery and we can enter DEFAULT MODE to parse it
//
// - Otherwise we are in an IN expression, so we jump back to EXPRESSION_MODE
mode IN_MODE;

AFTER_IN_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

AFTER_IN_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

AFTER_IN_WS
    : WS -> channel(HIDDEN)
    ;

// Match `( <hidden>* keyword <hidden>*` as lookahead, but emit only `(` as LP
// and rewind so the keyword is re-lexed in DEFAULT_MODE. The `<hidden>` group
// covers whitespace AND comments — without LINE_COMMENT/MULTILINE_COMMENT here
// the rule wouldn't fire for `IN ( /* note */ FROM foo )`, even though those
// tokens go to the hidden channel everywhere else they appear.
//
// `mode(DEFAULT_MODE)` replaces (rather than pushes) so the stack depth matches
// the EXPRESSION_MODE → DEFAULT_MODE pairing the existing FROM_RP / PROJECT_RP /
// etc. (each `popMode, popMode`) expect when closing the subquery.
// TODO: drop the {this.isDevVersion()}? predicate when WHERE_IN_SUBQUERY graduates
// to production (see EsqlCapabilities.WHERE_IN_SUBQUERY).
IN_SUBQUERY_LP
    : {this.isDevVersion()}?
      '(' (WS | LINE_COMMENT | MULTILINE_COMMENT)*
      ('from' | 'row' | 'show' | 'ts' | 'promql')
      { this.rewindToTokenStart(1); }
      -> type(LP), mode(DEFAULT_MODE)
    ;

// Match a single char as lookahead, then fully unwind so EXPRESSION_MODE
// re-lexes from the original position.
IN_EXPR_FALLBACK
    : . { this.rewindToTokenStart(0); } -> skip, popMode
    ;

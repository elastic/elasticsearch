/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

lexer grammar Inline;

INLINE : 'inline' -> pushMode(INLINE_MODE);
INLINESTATS : 'inlinestats' -> pushMode(EXPRESSION_MODE); // TODO: drop after next minor release

mode INLINE_MODE;

INLINE_STATS: 'stats' -> popMode, pushMode(EXPRESSION_MODE);

INLINE_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

INLINE_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

INLINE_WS
    : WS -> channel(HIDDEN)
    ;



/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Explain;

//
// Explain
//
EXPLAIN : 'explain'           -> pushMode(EXPLAIN_MODE);


mode EXPLAIN_MODE;
EXPLAIN_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(DEFAULT_MODE);
EXPLAIN_PIPE : PIPE -> type(PIPE), popMode;
EXPLAIN_WS : WS -> channel(HIDDEN);
EXPLAIN_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
EXPLAIN_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);

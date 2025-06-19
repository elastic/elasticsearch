/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Fromm;

//
// FROMM
//
FROMM : 'FROMM' -> pushMode(FROMM_MODE);

mode FROMM_MODE;
// commands needs to break out of their mode and the default mode when they encounter RP
FROMM_LP : LP -> type(LP), pushMode(DEFAULT_MODE);
// explicit popMode of RP to allow FROMM in FROMM branches
FROMM_RP : RP -> type(RP), popMode, popMode;
FROMM_PIPE : PIPE -> type(PIPE), popMode;

FROMM_WS : WS -> channel(HIDDEN);
FROMM_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
FROMM_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);


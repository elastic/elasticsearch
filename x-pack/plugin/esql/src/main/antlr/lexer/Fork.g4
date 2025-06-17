/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Fork;

//
// Fork
//
DEV_FORK :        {this.isDevVersion()}? 'fork'          -> pushMode(FORK_MODE);

mode FORK_MODE;
// commands needs to break out of their mode and the default mode when they encounter RP
FORK_LP : LP -> type(LP), pushMode(DEFAULT_MODE);
// explicit popMode of RP to allow FORK in FORK branches
FORK_RP : RP -> type(RP), popMode, popMode;
FORK_PIPE : PIPE -> type(PIPE), popMode;

FORK_WS : WS -> channel(HIDDEN);
FORK_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
FORK_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);


/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

lexer grammar Fuse;

FUSE : 'fuse' -> pushMode(FUSE_MODE);

mode FUSE_MODE;
FUSE_PIPE : PIPE -> type(PIPE), popMode;
// explicit popMode of RP to allow FUSE in FORK branches
FUSE_RP : RP -> type(RP), popMode, popMode;

GROUP: 'group';
SCORE: 'score';
KEY : 'key';

FUSE_WITH: WITH -> type(WITH), popMode, pushMode(EXPRESSION_MODE);
FUSE_COMMA: COMMA -> type(COMMA);
FUSE_DOT: DOT -> type(DOT);
FUSE_PARAM : PARAM -> type(PARAM);
FUSE_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
FUSE_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
FUSE_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);
FUSE_BY: BY -> type(BY);
FUSE_QUOTED_IDENTIFIER: QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
FUSE_UNQUOTED_IDENTIFIER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
FUSE_LINE_COMMENT: LINE_COMMENT -> channel(HIDDEN);
FUSE_MULTILINE_COMMENT: MULTILINE_COMMENT -> channel(HIDDEN);
FUSE_WS: WS -> channel(HIDDEN);

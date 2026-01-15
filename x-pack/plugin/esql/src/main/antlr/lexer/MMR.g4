/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

lexer grammar MMR;

DEV_MMR : {this.isDevVersion()}? 'mmr' -> pushMode(MMR_MODE);

mode MMR_MODE;

MMR_LIMIT: 'limit' -> popMode, pushMode(EXPRESSION_MODE);
MMR_TEXT_EMBEDDING: 'text_embedding';

MMR_UNQUOTED_IDENTIFIER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);

MMR_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

MMR_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

MMR_WS
    : WS -> channel(HIDDEN)
    ;



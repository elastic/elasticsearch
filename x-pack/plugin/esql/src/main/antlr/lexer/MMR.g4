/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

lexer grammar MMR;

DEV_MMR : {this.isDevVersion()}? 'mmr' -> pushMode(MMR_MODE);

mode MMR_MODE;

MMR_LIMIT: LIMIT -> popMode, pushMode(EXPRESSION_MODE);

MMR_ON: ON -> type(ON);
MMR_UNQUOTED_IDENTIFIER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
MMR_OPENING_BRACKET: OPENING_BRACKET -> type(OPENING_BRACKET);
MMR_CLOSING_BRACKET: CLOSING_BRACKET -> type(CLOSING_BRACKET);
MMR_COMMA: COMMA -> type(COMMA);
MMR_PLUS: PLUS -> type(PLUS);
MMR_MINUS: MINUS -> type(MINUS);
MMR_DECIMAL_LITERAL: DECIMAL_LITERAL -> type(DECIMAL_LITERAL);
MMR_INTEGER_LITERAL : INTEGER_LITERAL -> type(INTEGER_LITERAL);
MMR_PARAM: PARAM -> type(PARAM);
MMR_NAMED_OR_POSITIONAL_PARAM: NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
MMR_LP: LP -> type(LP);
MMR_RP: RP -> type(RP);
MMR_QUOTED_STRING: QUOTED_STRING -> type(QUOTED_STRING);
MMR_CAST_OP: CAST_OP -> type(CAST_OP);

MMR_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

MMR_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

MMR_WS
    : WS -> channel(HIDDEN)
    ;



/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Sequence;

DEV_SEQUENCE : {this.isDevVersion()}? 'sequence' -> pushMode(SEQUENCE_MODE);

mode SEQUENCE_MODE;
// The header stays in its own mode so '(' starts a FROM subquery, rather than
// an expression. Each stage command's RP returns to this mode.
SEQUENCE_BY : BY -> type(BY);
SEQUENCE_WITH : WITH -> type(WITH);
SEQUENCE_LP : LP -> type(LP), pushMode(DEFAULT_MODE);
SEQUENCE_RP : RP -> type(RP), popMode, popMode;
SEQUENCE_PIPE : PIPE -> type(PIPE), popMode;
SEQUENCE_COMMA : COMMA -> type(COMMA);
SEQUENCE_DOT : DOT -> type(DOT);
SEQUENCE_COLON : COLON -> type(COLON);
SEQUENCE_LEFT_BRACES : LEFT_BRACES -> type(LEFT_BRACES);
SEQUENCE_RIGHT_BRACES : RIGHT_BRACES -> type(RIGHT_BRACES);
SEQUENCE_STRING : QUOTED_STRING -> type(QUOTED_STRING);
SEQUENCE_INTEGER : INTEGER_LITERAL -> type(INTEGER_LITERAL);
SEQUENCE_DECIMAL : DECIMAL_LITERAL -> type(DECIMAL_LITERAL);
SEQUENCE_MINUS : MINUS -> type(MINUS);
SEQUENCE_PLUS : PLUS -> type(PLUS);
SEQUENCE_NULL : NULL -> type(NULL);
SEQUENCE_TRUE : TRUE -> type(TRUE);
SEQUENCE_FALSE : FALSE -> type(FALSE);
SEQUENCE_IDENTIFIER : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
SEQUENCE_QUOTED_IDENTIFIER : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
SEQUENCE_WS : WS -> channel(HIDDEN);
SEQUENCE_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
SEQUENCE_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);

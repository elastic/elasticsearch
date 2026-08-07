/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar DenseVector;

//
// DENSE_VECTOR command
//
// Uses a dedicated mode (mirroring PROJECT_MODE used by KEEP/DROP) so the field list accepts wildcard
// patterns (ID_PATTERN). The WITH { ... } options map switches to EXPRESSION_MODE, where the map tokens
// ({ } : strings) are lexed, exactly like COMPLETION/RERANK options.
DEV_DENSE_VECTOR : {this.isDevVersion()}? 'dense_vector' -> pushMode(DENSE_VECTOR_MODE);

mode DENSE_VECTOR_MODE;
DENSE_VECTOR_PIPE : PIPE -> type(PIPE), popMode;
// explicit popMode of RP to allow DENSE_VECTOR in FORK branches
DENSE_VECTOR_RP : RP -> type(RP), popMode, popMode;
// leave the field-pattern mode so the WITH { ... } options map is lexed as an expression
DENSE_VECTOR_WITH : WITH -> type(WITH), mode(EXPRESSION_MODE);
DENSE_VECTOR_DOT: DOT -> type(DOT);
DENSE_VECTOR_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET);
DENSE_VECTOR_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET);
DENSE_VECTOR_COMMA : COMMA -> type(COMMA);
DENSE_VECTOR_PARAM : PARAM -> type(PARAM);
DENSE_VECTOR_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
DENSE_VECTOR_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
DENSE_VECTOR_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

DENSE_VECTOR_ID_PATTERN : ID_PATTERN -> type(ID_PATTERN);

DENSE_VECTOR_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

DENSE_VECTOR_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

DENSE_VECTOR_WS
    : WS -> channel(HIDDEN)
    ;

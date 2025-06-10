/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Rename;

//
// | RENAME a.b AS x, c AS y
//
RENAME : 'rename'             -> pushMode(RENAME_MODE);


mode RENAME_MODE;
RENAME_PIPE : PIPE -> type(PIPE), popMode;
RENAME_ASSIGN : ASSIGN -> type(ASSIGN);
RENAME_COMMA : COMMA -> type(COMMA);
RENAME_DOT: DOT -> type(DOT);
RENAME_PARAM : PARAM -> type(PARAM);
RENAME_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
RENAME_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
RENAME_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

AS: 'as';

RENAME_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

RENAME_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

RENAME_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

RENAME_WS
    : WS -> channel(HIDDEN)
    ;

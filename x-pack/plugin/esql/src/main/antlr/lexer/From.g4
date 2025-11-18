/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar From;

//
// FROM command
//
FROM : 'from'                 -> pushMode(FROM_MODE);

// TS command
TS : 'ts' -> pushMode(FROM_MODE);

mode FROM_MODE;
FROM_PIPE : PIPE -> type(PIPE), popMode;
FROM_COLON : COLON -> type(COLON);
FROM_SELECTOR : CAST_OP -> type(CAST_OP);
FROM_COMMA : COMMA -> type(COMMA);
FROM_ASSIGN : ASSIGN -> type(ASSIGN);
METADATA : 'metadata';

// we need this for EXPLAIN
// change to double popMode to accommodate subquerys in FROM, when see ')' pop out of subquery(default) mode and from mode
FROM_RP : RP -> type(RP), popMode, popMode;

// accommodate subQuery inside FROM
FROM_LP : LP -> type(LP), pushMode(DEFAULT_MODE);

// in 8.14 ` were not allowed
// this has been relaxed in 8.15 since " is used for quoting
fragment UNQUOTED_SOURCE_PART
    : ~[:"=|,[\]/() \t\r\n]
    | '/' ~[*/] // allow single / but not followed by another / or * which would start a comment -- used in index pattern date spec
    ;

UNQUOTED_SOURCE
    : UNQUOTED_SOURCE_PART+
    ;

FROM_UNQUOTED_SOURCE : UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
FROM_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);

FROM_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

FROM_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

FROM_WS
    : WS -> channel(HIDDEN)
    ;

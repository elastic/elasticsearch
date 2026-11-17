/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Into;

DEV_INTO : 'into' {this.isDevVersion()}? -> pushMode(INTO_MODE);

mode INTO_MODE;
INTO_PIPE : PIPE -> type(PIPE), popMode;
// Pop both INTO_MODE and the enclosing subquery/FORK mode on ')' so the parser sees and rejects the misplaced INTO.
INTO_RP : RP -> type(RP), popMode, popMode;
INTO_UNQUOTED_SOURCE : UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
INTO_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);
INTO_COLON : COLON -> type(COLON);
INTO_SELECTOR : CAST_OP -> type(CAST_OP);

INTO_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

INTO_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

INTO_WS
    : WS -> channel(HIDDEN)
    ;

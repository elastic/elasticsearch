/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Join;

//
// JOIN-related commands
//
JOIN_LOOKUP : 'lookup'        -> pushMode(JOIN_MODE);
DEV_JOIN_FULL :   {this.isDevVersion()}? 'full'          -> pushMode(JOIN_MODE);
DEV_JOIN_LEFT :   {this.isDevVersion()}? 'left'          -> pushMode(JOIN_MODE);
DEV_JOIN_RIGHT :  {this.isDevVersion()}? 'right'         -> pushMode(JOIN_MODE);

mode JOIN_MODE;
JOIN_PIPE : PIPE -> type(PIPE), popMode;
JOIN : 'join';
JOIN_AS : AS -> type(AS);
JOIN_ON : ON -> type(ON), popMode, pushMode(EXPRESSION_MODE);
USING : 'USING' -> popMode, pushMode(EXPRESSION_MODE);

JOIN_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
JOIN_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);
JOIN_COLON : COLON -> type(COLON);

JOIN_UNQUOTED_IDENTIFER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
JOIN_QUOTED_IDENTIFIER : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);

JOIN_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

JOIN_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

JOIN_WS
    : WS -> channel(HIDDEN)
    ;

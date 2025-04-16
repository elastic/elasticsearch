/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar ChangePoint;

//
// | CHANGE_POINT command
//
CHANGE_POINT : 'change_point' -> pushMode(CHANGE_POINT_MODE);

mode CHANGE_POINT_MODE;
CHANGE_POINT_PIPE : PIPE -> type(PIPE), popMode;
CHANGE_POINT_ON : ON -> type(ON);
CHANGE_POINT_AS : AS -> type(AS);
CHANGE_POINT_DOT: DOT -> type(DOT);
CHANGE_POINT_COMMA: COMMA -> type(COMMA);
CHANGE_POINT_QUOTED_IDENTIFIER: QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER);
CHANGE_POINT_UNQUOTED_IDENTIFIER: UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
CHANGE_POINT_LINE_COMMENT: LINE_COMMENT -> channel(HIDDEN);
CHANGE_POINT_MULTILINE_COMMENT: MULTILINE_COMMENT -> channel(HIDDEN);
CHANGE_POINT_WS: WS -> channel(HIDDEN);

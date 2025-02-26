/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Unpivot;

//
// | UNPIVOT nameColumn FOR valueColumn IN (fields*)
//
DEV_UNPIVOT :     {this.isDevVersion()}? 'unpivot'       -> pushMode(UNPIVOT_MODE);

mode UNPIVOT_MODE;
UNPIVOT_PIPE : PIPE -> type(PIPE), popMode;
UNPIVOT_COLON : COLON -> type(COLON);
UNPIVOT_COMMA : COMMA -> type(COMMA);
UNPIVOT_DOT: DOT -> type(DOT);
UNPIVOT_ON : IN -> type(IN), pushMode(UNPIVOT_FIELD_MODE);
UNPIVOT_FOR : FOR -> type(FOR);

UNPIVOT_ID_PATTERN : ID_PATTERN -> type(ID_PATTERN);
UNPIVOT_UNQUOTED_SOURCE: UNQUOTED_SOURCE -> type(UNQUOTED_SOURCE);
UNPIVOT_QUOTED_SOURCE : QUOTED_STRING -> type(QUOTED_STRING);

UNPIVOT_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

UNPIVOT_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

UNPIVOT_WS
    : WS -> channel(HIDDEN)
    ;

mode UNPIVOT_FIELD_MODE;
UNPIVOT_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
UNPIVOT_FIELD_COMMA : COMMA -> type(COMMA);
UNPIVOT_FIELD_DOT: DOT -> type(DOT);
UNPIVOT_FIELD_LP : LP -> type(LP);
UNPIVOT_FIELD_RP : RP -> type(RP);

UNPIVOT_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

UNPIVOT_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

UNPIVOT_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

UNPIVOT_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Enrich;

//
// | ENRICH ON key WITH fields
//
ENRICH : 'enrich'             -> pushMode(ENRICH_MODE);


mode ENRICH_MODE;
ENRICH_PIPE : PIPE -> type(PIPE), popMode;
ENRICH_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(SETTING_MODE);

ENRICH_ON : ON -> type(ON), pushMode(ENRICH_FIELD_MODE);
ENRICH_WITH : WITH -> type(WITH), pushMode(ENRICH_FIELD_MODE);

// similar to that of an index
// see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
fragment ENRICH_POLICY_NAME_BODY
    : ~[\\/?"<>| ,#\t\r\n:]
    ;

ENRICH_POLICY_NAME
    // allow prefix for the policy to specify its resolution
    : (ENRICH_POLICY_NAME_BODY+ COLON)? ENRICH_POLICY_NAME_BODY+
    ;

ENRICH_MODE_UNQUOTED_VALUE
    : ENRICH_POLICY_NAME -> type(ENRICH_POLICY_NAME)
    ;

ENRICH_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_WS
    : WS -> channel(HIDDEN)
    ;

// submode for Enrich to allow different lexing between policy source (loose) and field identifiers
mode ENRICH_FIELD_MODE;
ENRICH_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
ENRICH_FIELD_ASSIGN : ASSIGN -> type(ASSIGN);
ENRICH_FIELD_COMMA : COMMA -> type(COMMA);
ENRICH_FIELD_DOT: DOT -> type(DOT);

ENRICH_FIELD_WITH : WITH -> type(WITH) ;

ENRICH_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

ENRICH_FIELD_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

ENRICH_FIELD_PARAM : PARAM -> type(PARAM);
ENRICH_FIELD_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
ENRICH_FIELD_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
ENRICH_FIELD_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

ENRICH_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

mode SETTING_MODE;
SETTING_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET), popMode;

SETTING_COLON : COLON -> type(COLON);

SETTING
    : (ASPERAND | DIGIT| DOT | LETTER | UNDERSCORE)+
    ;

SETTING_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SETTTING_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SETTING_WS
    : WS -> channel(HIDDEN)
    ;

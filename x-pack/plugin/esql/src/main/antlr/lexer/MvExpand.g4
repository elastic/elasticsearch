/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar MvExpand;

//
// MV_EXPAND
//
MV_EXPAND : 'mv_expand'       -> pushMode(MVEXPAND_MODE);


mode MVEXPAND_MODE;
MVEXPAND_PIPE : PIPE -> type(PIPE), popMode;
MVEXPAND_DOT: DOT -> type(DOT);
MVEXPAND_PARAM : PARAM -> type(PARAM);
MVEXPAND_NAMED_OR_POSITIONAL_PARAM : NAMED_OR_POSITIONAL_PARAM -> type(NAMED_OR_POSITIONAL_PARAM);
MVEXPAND_DOUBLE_PARAMS : DOUBLE_PARAMS -> type(DOUBLE_PARAMS);
MVEXPAND_NAMED_OR_POSITIONAL_DOUBLE_PARAMS : NAMED_OR_POSITIONAL_DOUBLE_PARAMS -> type(NAMED_OR_POSITIONAL_DOUBLE_PARAMS);

MVEXPAND_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

MVEXPAND_UNQUOTED_IDENTIFIER
    : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER)
    ;

MVEXPAND_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

MVEXPAND_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

MVEXPAND_WS
    : WS -> channel(HIDDEN)
    ;

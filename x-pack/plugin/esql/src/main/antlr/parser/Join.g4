/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Join;

joinCommand
    : type=(JOIN_LOOKUP | DEV_JOIN_LEFT | DEV_JOIN_RIGHT) JOIN joinTarget joinCondition
    ;

joinTarget
    // Cannot use UNQUOTED_IDENTIFIER for the qualifier because the lexer will confuse it with an UNQUOTED_SOURCE; would
    // require pushing a mode to the lexer to disambiguate.
    : {this.isDevVersion()}? index=indexPattern AS? qualifier=UNQUOTED_SOURCE
    | index=indexPattern
    ;

joinCondition
    : ON booleanExpression (COMMA booleanExpression)*
    ;




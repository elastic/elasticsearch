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
    // TODO: UNQUOTED_SOURCE should be restricted further - and we should have reserved
    // characters for internal usage.
    : index=indexPattern qualifier=UNQUOTED_SOURCE?
    | {this.isDevVersion()}? index=indexPattern (AS? qualifier=UNQUOTED_SOURCE)?
    ;

joinCondition
    : ON joinPredicate (COMMA joinPredicate)*
    ;

joinPredicate
    : valueExpression
    ;



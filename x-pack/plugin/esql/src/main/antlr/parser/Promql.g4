/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Promql;

promqlCommand
    : DEV_PROMQL promqlParam+ LP promqlQueryPart* RP
    ;

promqlParam
    : name=promqlParamContent value=promqlParamContent
    ;

promqlParamContent
    : PROMQL_UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    | QUOTED_STRING
    | NAMED_OR_POSITIONAL_PARAM
    ;

promqlQueryPart
    : PROMQL_QUERY_TEXT           // Regular text
    | LP promqlQueryPart* RP    // Nested parens (recursive!)
    ;

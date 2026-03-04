/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Promql;

promqlCommand
    : PROMQL promqlParam* (valueName ASSIGN)? LP promqlQueryPart+ RP
    | PROMQL promqlParam* promqlQueryPart+
    ;

valueName
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

promqlParam
    : name=promqlParamName ASSIGN value=promqlParamValue
    ;

promqlParamName
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    | QUOTED_STRING
    | NAMED_OR_POSITIONAL_PARAM
    ;

promqlParamValue
    : promqlIndexPattern (COMMA promqlIndexPattern)*
    | QUOTED_IDENTIFIER
    | NAMED_OR_POSITIONAL_PARAM
    ;

promqlQueryContent
    : UNQUOTED_SOURCE
    | UNQUOTED_IDENTIFIER
    | QUOTED_STRING
    | QUOTED_IDENTIFIER
    | NAMED_OR_POSITIONAL_PARAM
    | PROMQL_QUERY_COMMENT
    | PROMQL_SINGLE_QUOTED_STRING
    | ASSIGN
    | COMMA
    | COLON
    | CAST_OP
    | PROMQL_OTHER_QUERY_CONTENT
    ;

promqlQueryPart
    : promqlQueryContent+        // Regular text
    | LP promqlQueryPart* RP    // Nested parens (recursive!)
    ;

//
// An adaptation of the indexPattern rule from EsqlBaseParser.g4
// We can't import that rule directly because the UNQUOTED_IDENTIFIER token would shadow the EsqlBaseLexer.UNQUOTED_SOURCE token
//
promqlIndexPattern
    : promqlClusterString COLON promqlUnquotedIndexString
    | promqlUnquotedIndexString CAST_OP promqlSelectorString
    | promqlIndexString
    ;

promqlClusterString
    : UNQUOTED_IDENTIFIER
    | UNQUOTED_SOURCE
    ;

promqlSelectorString
    : UNQUOTED_IDENTIFIER
    | UNQUOTED_SOURCE
    ;

promqlUnquotedIndexString
    : UNQUOTED_IDENTIFIER
    | UNQUOTED_SOURCE
    ;

promqlIndexString
    : UNQUOTED_IDENTIFIER
    | UNQUOTED_SOURCE
    | QUOTED_STRING
    ;
// ---

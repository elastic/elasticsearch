/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar EsqlBaseParser;

@header {
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
}

options {
  superClass=ParserConfig;
  tokenVocab=EsqlBaseLexer;
}

import Expression,
       Join;

singleStatement
    : query EOF
    ;

query
    : sourceCommand                 #singleCommandQuery
    | query PIPE processingCommand  #compositeQuery
    ;

sourceCommand
    : explainCommand
    | fromCommand
    | rowCommand
    | showCommand
    // in development
    | {this.isDevVersion()}? timeSeriesCommand
    ;

processingCommand
    : evalCommand
    | whereCommand
    | keepCommand
    | limitCommand
    | statsCommand
    | sortCommand
    | dropCommand
    | renameCommand
    | dissectCommand
    | grokCommand
    | enrichCommand
    | mvExpandCommand
    | joinCommand
    | changePointCommand
    // in development
    | {this.isDevVersion()}? inlinestatsCommand
    | {this.isDevVersion()}? lookupCommand
    | {this.isDevVersion()}? completionCommand
    | {this.isDevVersion()}? insistCommand
    | {this.isDevVersion()}? forkCommand
    | {this.isDevVersion()}? rerankCommand
    | {this.isDevVersion()}? rrfCommand
    | {this.isDevVersion()}? sampleCommand
    ;

whereCommand
    : WHERE booleanExpression
    ;

dataType
    : identifier                                                                        #toDataType
    ;

rowCommand
    : ROW fields
    ;

fields
    : field (COMMA field)*
    ;

field
    : (qualifiedName ASSIGN)? booleanExpression
    ;

rerankFields
    : rerankField (COMMA rerankField)*
    ;

rerankField
    : qualifiedName (ASSIGN booleanExpression)?
    ;

fromCommand
    : FROM indexPatternAndMetadataFields
    ;

timeSeriesCommand
    : DEV_TIME_SERIES indexPatternAndMetadataFields
    ;

indexPatternAndMetadataFields:
    indexPattern (COMMA indexPattern)* metadata?
    ;

indexPattern
    : (clusterString COLON)? indexString
    | indexString (CAST_OP selectorString)?
    ;

clusterString
    : UNQUOTED_SOURCE
    | QUOTED_STRING
    ;

selectorString
    : UNQUOTED_SOURCE
    | QUOTED_STRING
    ;

indexString
    : UNQUOTED_SOURCE
    | QUOTED_STRING
    ;

metadata
    : METADATA UNQUOTED_SOURCE (COMMA UNQUOTED_SOURCE)*
    ;

evalCommand
    : EVAL fields
    ;

statsCommand
    : STATS stats=aggFields? (BY grouping=fields)?
    ;

aggFields
    : aggField (COMMA aggField)*
    ;

aggField
    : field (WHERE booleanExpression)?
    ;

qualifiedName
    : identifierOrParameter (DOT identifierOrParameter)*
    ;

qualifiedNamePattern
    : identifierPattern (DOT identifierPattern)*
    ;

qualifiedNamePatterns
    : qualifiedNamePattern (COMMA qualifiedNamePattern)*
    ;

identifier
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

identifierPattern
    : ID_PATTERN
    | parameter
    | doubleParameter
    ;

parameter
    : PARAM                        #inputParam
    | NAMED_OR_POSITIONAL_PARAM    #inputNamedOrPositionalParam
    ;

doubleParameter
    : DOUBLE_PARAMS                        #inputDoubleParams
    | NAMED_OR_POSITIONAL_DOUBLE_PARAMS    #inputNamedOrPositionalDoubleParams
    ;

identifierOrParameter
    : identifier
    | parameter
    | doubleParameter
    ;

limitCommand
    : LIMIT INTEGER_LITERAL
    ;

sortCommand
    : SORT orderExpression (COMMA orderExpression)*
    ;

orderExpression
    : booleanExpression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
    ;

keepCommand
    :  KEEP qualifiedNamePatterns
    ;

dropCommand
    : DROP qualifiedNamePatterns
    ;

renameCommand
    : RENAME renameClause (COMMA renameClause)*
    ;

renameClause:
    oldName=qualifiedNamePattern AS newName=qualifiedNamePattern
    ;

dissectCommand
    : DISSECT primaryExpression string commandOptions?
    ;

grokCommand
    : GROK primaryExpression string
    ;

mvExpandCommand
    : MV_EXPAND qualifiedName
    ;

commandOptions
    : commandOption (COMMA commandOption)*
    ;

commandOption
    : identifier ASSIGN constant
    ;

explainCommand
    : EXPLAIN subqueryExpression
    ;

subqueryExpression
    : OPENING_BRACKET query CLOSING_BRACKET
    ;

showCommand
    : SHOW INFO                                                           #showInfo
    ;

enrichCommand
    : ENRICH policyName=ENRICH_POLICY_NAME (ON matchField=qualifiedNamePattern)? (WITH enrichWithClause (COMMA enrichWithClause)*)?
    ;

enrichWithClause
    : (newName=qualifiedNamePattern ASSIGN)? enrichField=qualifiedNamePattern
    ;

//
// In development
//
lookupCommand
    : DEV_LOOKUP tableName=indexPattern ON matchFields=qualifiedNamePatterns
    ;

inlinestatsCommand
    : DEV_INLINESTATS stats=aggFields (BY grouping=fields)?
    ;

changePointCommand
    : CHANGE_POINT value=qualifiedName (ON key=qualifiedName)? (AS targetType=qualifiedName COMMA targetPvalue=qualifiedName)?
    ;

insistCommand
    : DEV_INSIST qualifiedNamePatterns
    ;

forkCommand
    : DEV_FORK forkSubQueries
    ;

forkSubQueries
    : (forkSubQuery)+
    ;

forkSubQuery
    : LP forkSubQueryCommand RP
    ;

forkSubQueryCommand
    : forkSubQueryProcessingCommand                             #singleForkSubQueryCommand
    | forkSubQueryCommand PIPE forkSubQueryProcessingCommand    #compositeForkSubQuery
    ;

forkSubQueryProcessingCommand
    : evalCommand
    | whereCommand
    | limitCommand
    | statsCommand
    | sortCommand
    | dissectCommand
    ;

rrfCommand
   : DEV_RRF
   ;

rerankCommand
    : DEV_RERANK queryText=constant ON rerankFields WITH inferenceId=identifierOrParameter
    ;

completionCommand
    : DEV_COMPLETION prompt=primaryExpression WITH inferenceId=identifierOrParameter (AS targetField=qualifiedName)?
    ;

sampleCommand
    : DEV_SAMPLE probability=decimalValue seed=integerValue?
    ;

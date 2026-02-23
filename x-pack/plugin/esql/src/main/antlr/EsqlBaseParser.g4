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
       Join,
       Promql;

statements
    : setCommand* singleStatement EOF
    ;

singleStatement
    : query EOF
    ;

query
    : sourceCommand                 #singleCommandQuery
    | query PIPE processingCommand  #compositeQuery
    ;

sourceCommand
    : fromCommand
    | rowCommand
    | showCommand
    | timeSeriesCommand
    | promqlCommand
    // in development
    | {this.isDevVersion()}? explainCommand
    | {this.isDevVersion()}? externalCommand
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
    | completionCommand
    | sampleCommand
    | forkCommand
    | rerankCommand
    | inlineStatsCommand
    | fuseCommand
    | uriPartsCommand
    | metricsInfoCommand
    // in development
    | {this.isDevVersion()}? lookupCommand
    | {this.isDevVersion()}? insistCommand
    | {this.isDevVersion()}? mmrCommand
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

fromCommand
    : FROM indexPatternAndMetadataFields
    ;

timeSeriesCommand
    : TS indexPatternAndMetadataFields
    ;

externalCommand
    : EXTERNAL stringOrParameter commandNamedParameters
    ;

indexPatternAndMetadataFields
    : indexPatternOrSubquery (COMMA indexPatternOrSubquery)* metadata?
    ;

indexPatternOrSubquery
    : indexPattern
    | {this.isDevVersion()}? subquery
    ;

subquery
    : LP fromCommand (PIPE processingCommand)* RP
    ;

indexPattern
    : (clusterString COLON)? unquotedIndexString (CAST_OP selectorString)?
    | indexString
    ;

clusterString
    : UNQUOTED_SOURCE
    ;

selectorString
    : UNQUOTED_SOURCE
    ;

unquotedIndexString
    : UNQUOTED_SOURCE
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
    : {this.isDevVersion()}? OPENING_BRACKET qualifier=UNQUOTED_IDENTIFIER? CLOSING_BRACKET DOT OPENING_BRACKET name=fieldName CLOSING_BRACKET
    | name=fieldName
    ;

fieldName
    : identifierOrParameter (DOT identifierOrParameter)*
    ;

qualifiedNamePattern
    : {this.isDevVersion()}? OPENING_BRACKET qualifier=ID_PATTERN? CLOSING_BRACKET DOT OPENING_BRACKET name=fieldNamePattern CLOSING_BRACKET
    | name=fieldNamePattern
    ;

fieldNamePattern
    : (identifierPattern (DOT identifierPattern)*)
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

stringOrParameter
    : string
    | parameter
    ;

limitCommand
    : LIMIT constant
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
    | newName=qualifiedNamePattern ASSIGN oldName=qualifiedNamePattern
    ;

dissectCommand
    : DISSECT primaryExpression string dissectCommandOptions?
    ;

dissectCommandOptions
    : dissectCommandOption (COMMA dissectCommandOption)*
    ;

dissectCommandOption
    : identifier ASSIGN constant
    ;


commandNamedParameters
    : (WITH mapExpression)?
    ;

grokCommand
    : GROK primaryExpression string (COMMA string)*
    ;

mvExpandCommand
    : MV_EXPAND qualifiedName
    ;

explainCommand
    : DEV_EXPLAIN subqueryExpression
    ;

subqueryExpression
    : LP query RP
    ;

showCommand
    : SHOW INFO                                                           #showInfo
    ;

enrichCommand
    : ENRICH policyName=enrichPolicyName (ON matchField=qualifiedNamePattern)? (WITH enrichWithClause (COMMA enrichWithClause)*)?
    ;

enrichPolicyName
    : ENRICH_POLICY_NAME
    | QUOTED_STRING
    ;

enrichWithClause
    : (newName=qualifiedNamePattern ASSIGN)? enrichField=qualifiedNamePattern
    ;

sampleCommand
    : SAMPLE probability=constant
    ;

changePointCommand
    : CHANGE_POINT value=qualifiedName (ON key=qualifiedName)? (AS targetType=qualifiedName COMMA targetPvalue=qualifiedName)?
    ;

forkCommand
    : FORK forkSubQueries
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
    : processingCommand
    ;

rerankCommand
    : RERANK (targetField=qualifiedName ASSIGN)? queryText=constant ON rerankFields=fields commandNamedParameters
    ;

completionCommand
    : COMPLETION (targetField=qualifiedName ASSIGN)? prompt=primaryExpression commandNamedParameters
    ;

inlineStatsCommand
    : INLINE INLINE_STATS stats=aggFields (BY grouping=fields)?
    // TODO: drop after next minor release
    | INLINESTATS stats=aggFields (BY grouping=fields)?
    ;

fuseCommand
    : FUSE (fuseType=identifier)? (fuseConfiguration)*
    ;

fuseConfiguration
    : SCORE BY score=qualifiedName
    | KEY BY key=fuseKeyByFields
    | GROUP BY group=qualifiedName
    | WITH options=mapExpression
    ;

fuseKeyByFields
   : qualifiedName (COMMA qualifiedName)*
   ;

metricsInfoCommand
    : METRICS_INFO
    ;

//
// In development
//
lookupCommand
    : DEV_LOOKUP tableName=indexPattern ON matchFields=qualifiedNamePatterns
    ;

insistCommand
    : DEV_INSIST qualifiedNamePatterns
    ;

uriPartsCommand
    : URI_PARTS qualifiedName ASSIGN primaryExpression
    ;

setCommand
    : SET setField SEMICOLON
    ;

setField
    : identifier ASSIGN ( constant | mapExpression )
    ;

mmrCommand
    :  DEV_MMR (queryVector=mmrQueryVectorParams)? ON diversifyField=qualifiedName MMR_LIMIT limitValue=integerValue commandNamedParameters
    ;

mmrQueryVectorParams
    : parameter                           # mmrQueryVectorParameter
    | primaryExpression                   # mmrQueryVectorExpression
    ;

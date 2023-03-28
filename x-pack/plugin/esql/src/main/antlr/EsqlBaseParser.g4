
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

parser grammar EsqlBaseParser;

options {tokenVocab=EsqlBaseLexer;}

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
    ;

processingCommand
    : evalCommand
    | inlinestatsCommand
    | limitCommand
    | projectCommand
    | sortCommand
    | statsCommand
    | whereCommand
    | dropCommand
    | dissectCommand
    ;

whereCommand
    : WHERE booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                               #logicalNot
    | valueExpression                                                     #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression         #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression          #logicalBinary
    ;

valueExpression
    : operatorExpression                                                                      #valueExpressionDefault
    | left=operatorExpression comparisonOperator right=operatorExpression                     #comparison
    ;

operatorExpression
    : primaryExpression                                                                       #operatorExpressionDefault
    | operator=(MINUS | PLUS) operatorExpression                                              #arithmeticUnary
    | left=operatorExpression operator=(ASTERISK | SLASH | PERCENT) right=operatorExpression  #arithmeticBinary
    | left=operatorExpression operator=(PLUS | MINUS) right=operatorExpression                #arithmeticBinary
    ;

primaryExpression
    : constant                                                                          #constantDefault
    | qualifiedName                                                                     #dereference
    | LP booleanExpression RP                                                           #parenthesizedExpression
    | identifier LP (booleanExpression (COMMA booleanExpression)*)? RP                  #functionExpression
    ;

rowCommand
    : ROW fields
    ;

fields
    : field (COMMA field)*
    ;

field
    : booleanExpression
    | qualifiedName ASSIGN booleanExpression
    ;

fromCommand
    : FROM sourceIdentifier (COMMA sourceIdentifier)*
    ;

evalCommand
    : EVAL fields
    ;

statsCommand
    : STATS fields (BY qualifiedNames)?
    ;

inlinestatsCommand
    : INLINESTATS fields (BY qualifiedNames)?
    ;

sourceIdentifier
    : SRC_UNQUOTED_IDENTIFIER
    | SRC_QUOTED_IDENTIFIER
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

qualifiedNames
    : qualifiedName (COMMA qualifiedName)*
    ;

identifier
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

constant
    : NULL                                                                              #nullLiteral
    | integerValue UNQUOTED_IDENTIFIER                                                  #qualifiedIntegerLiteral
    | decimalValue                                                                      #decimalLiteral
    | integerValue                                                                      #integerLiteral
    | booleanValue                                                                      #booleanLiteral
    | string                                                                            #stringLiteral
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

projectCommand
    :  PROJECT projectClause (COMMA projectClause)*
    ;

projectClause
    : sourceIdentifier
    | newName=sourceIdentifier ASSIGN oldName=sourceIdentifier
    ;

dropCommand
    : DROP sourceIdentifier (COMMA sourceIdentifier)*
    ;

dissectCommand
    : DISSECT primaryExpression string commandOptions?
    ;

commandOptions
    : commandOption (COMMA commandOption)*
    ;

commandOption
    : identifier ASSIGN constant
    ;

booleanValue
    : TRUE | FALSE
    ;

decimalValue
    : DECIMAL_LITERAL
    ;

integerValue
    : INTEGER_LITERAL
    ;

string
    : STRING
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

explainCommand
    : EXPLAIN subqueryExpression
    ;

subqueryExpression
    : OPENING_BRACKET query CLOSING_BRACKET
    ;

showCommand
    : SHOW INFO                                                           #showInfo
    | SHOW FUNCTIONS                                                      #showFunctions
    ;

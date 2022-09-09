
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
    : sourceCommand pipe*
    ;

pipe
    : PIPE processingCommand
    ;

sourceCommand
    : rowCommand
    | fromCommand
    ;

processingCommand
    : whereCommand
    | limitCommand
    | sortCommand
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
    ;

rowCommand
    : ROW fields
    ;

fields
    : field (COMMA field)*
    ;

field
    : constant
    | qualifiedName ASSIGN constant
    ;

fromCommand
    : FROM sourceIdentifier (COMMA sourceIdentifier)*
    ;

sourceIdentifier
    : SRC_UNQUOTED_IDENTIFIER
    | SRC_QUOTED_IDENTIFIER
    ;

qualifiedName
    : identifier (DOT identifier)*
    ;

identifier
    : UNQUOTED_IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

constant
    : NULL                                                                              #nullLiteral
    | number                                                                            #numericLiteral
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

booleanValue
    : TRUE | FALSE
    ;

number
    : DECIMAL_LITERAL  #decimalLiteral
    | INTEGER_LITERAL  #integerLiteral
    ;

string
    : STRING
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

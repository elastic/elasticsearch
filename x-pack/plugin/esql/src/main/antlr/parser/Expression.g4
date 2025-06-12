/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Expression;

booleanExpression
    : NOT booleanExpression                                                      #logicalNot
    | valueExpression                                                            #booleanDefault
    | regexBooleanExpression                                                     #regexExpression
    | left=booleanExpression operator=AND right=booleanExpression                #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                 #logicalBinary
    | valueExpression (NOT)? IN LP valueExpression (COMMA valueExpression)* RP   #logicalIn
    | valueExpression IS NOT? NULL                                               #isNull
    | matchBooleanExpression                                                     #matchExpression
    ;

regexBooleanExpression
    : valueExpression (NOT)? LIKE string                               #likeExpression
    | valueExpression (NOT)? RLIKE string                                 #rlikeExpression
    | valueExpression (NOT)? LIKE LP string  (COMMA string )* RP  #logicalLikeList
    ;

matchBooleanExpression
    : fieldExp=qualifiedName (CAST_OP fieldType=dataType)? COLON matchQuery=constant
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
    | functionExpression                                                                #function
    | LP booleanExpression RP                                                           #parenthesizedExpression
    | primaryExpression CAST_OP dataType                                                #inlineCast
    ;

functionExpression
    : functionName LP (ASTERISK | (booleanExpression (COMMA booleanExpression)* (COMMA mapExpression)?))? RP
    ;

functionName
    : identifierOrParameter
    ;

mapExpression
    : LEFT_BRACES entryExpression (COMMA entryExpression)* RIGHT_BRACES
    ;

entryExpression
    : key=string COLON value=constant
    ;

constant
    : NULL                                                                              #nullLiteral
    | integerValue UNQUOTED_IDENTIFIER                                                  #qualifiedIntegerLiteral
    | decimalValue                                                                      #decimalLiteral
    | integerValue                                                                      #integerLiteral
    | booleanValue                                                                      #booleanLiteral
    | parameter                                                                         #inputParameter
    | string                                                                            #stringLiteral
    | OPENING_BRACKET numericValue (COMMA numericValue)* CLOSING_BRACKET                #numericArrayLiteral
    | OPENING_BRACKET booleanValue (COMMA booleanValue)* CLOSING_BRACKET                #booleanArrayLiteral
    | OPENING_BRACKET string (COMMA string)* CLOSING_BRACKET                            #stringArrayLiteral
    ;

booleanValue
    : TRUE | FALSE
    ;

numericValue
    : decimalValue
    | integerValue
    ;

decimalValue
    : (PLUS | MINUS)? DECIMAL_LITERAL
    ;

integerValue
    : (PLUS | MINUS)? INTEGER_LITERAL
    ;

string
    : QUOTED_STRING
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

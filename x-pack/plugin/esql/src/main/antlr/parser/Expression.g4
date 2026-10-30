/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar Expression;

booleanExpression
    : NOT booleanExpression                                                                    #logicalNot
    | valueExpression predicate?                                                               #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression                              #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                               #logicalBinary
    ;

predicate
    : (NOT)? IN LP valueExpression (COMMA valueExpression)* RP                                 #logicalIn
    | (NOT)? IN subquery                                                                       #logicalInSubquery
    | IS NOT? NULL                                                                             #isNull
    // the list forms need two or more patterns and come first: a parenthesized list of strings is also a valid primaryExpression,
    // and ANTLR resolves that ambiguity in favour of the first alternative. A single pattern in parentheses is a primaryExpression.
    | (NOT)? LIKE LP stringOrParameter (COMMA stringOrParameter )+ RP                          #likeListExpression
    | (NOT)? RLIKE LP stringOrParameter (COMMA stringOrParameter )+ RP                         #rlikeListExpression
    | (NOT)? LIKE primaryExpression                                                            #likeExpression
    | (NOT)? RLIKE primaryExpression                                                           #rlikeExpression
    | COLON matchQuery=constant                                                                #matchExpression
    ;

valueExpression
    : left=operatorExpression (comparisonOperator right=operatorExpression)?
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
    // more than one expression is only valid as the left side of a multi-column IN subquery, see ExpressionBuilder
    | LP booleanExpression (COMMA booleanExpression)* RP                                #parenthesizedExpression
    | primaryExpression CAST_OP dataType                                                #inlineCast
    ;

functionExpression
    : functionName LP (ASTERISK | ((booleanExpression | lambda) (COMMA (booleanExpression | lambda))* (COMMA mapExpression)?))? RP
    ;

functionName
    : identifierOrParameter
    | FIRST
    | LAST
    ;

lambda
    : {this.isDevVersion()}? LP (identifier (COMMA identifier)*)? RP ARROW booleanExpression
    | {this.isDevVersion()}? identifier ARROW booleanExpression
    ;

mapExpression
    : LEFT_BRACES (entryExpression (COMMA entryExpression)*)? RIGHT_BRACES
    ;

entryExpression
    : key=string COLON value=mapValue
    ;

mapValue
    : constant
    | mapExpression
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

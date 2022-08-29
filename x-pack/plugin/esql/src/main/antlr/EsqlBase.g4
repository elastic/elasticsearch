/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

grammar EsqlBase;

singleStatement
    : query
    ;

singleExpression
    : expression EOF
    ;

query
    : sourceCommand (PIPE processingCommand)*
    ;

sourceCommand
    : rowCommand
    | fromCommand
    ;

processingCommand
    : whereCommand
    ;

whereCommand
    : WHERE expression
    ;

expression
    : booleanExpression
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
    | LP expression RP                                                                  #parenthesizedExpression
    ;

rowCommand
    : ROW fields
    ;

fields
    : field (COMMA field)*
    ;

field
    : constant
    | qualifiedName ASGN constant
    ;

fromCommand
    : FROM wildcardIdentifier (COMMA wildcardIdentifier)*
    ;

qualifiedName
    : wildcardIdentifier (DOT wildcardIdentifier)*
    ;

wildcardIdentifier
    : IDENTIFIER
    | QUOTED_IDENTIFIER
    ;

constant
    : NULL                                                                              #nullLiteral
    | number                                                                            #numericLiteral
    | booleanValue                                                                      #booleanLiteral
    | string                                                                            #stringLiteral
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

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

fragment STRING_ESCAPE
    : '\\' [btnfr"'\\]
    ;

fragment UNESCAPED_CHARS
    : ~[\r\n"\\]
    ;

fragment EXPONENT
    : [Ee] [+-]? DIGIT+
    ;

fragment UNQUOTED_IDENTIFIER
    : ~[`|., \t\r\n]*
    ;

STRING
    : '"' (STRING_ESCAPE | UNESCAPED_CHARS)* '"'
    | '"""' (~[\r\n])*? '"""' '"'? '"'?
    ;

INTEGER_LITERAL
    : DIGIT+
    ;

DECIMAL_LITERAL
    : DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    | DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT
    ;

AND : 'and';
ASGN : '=';
COMMA : ',';
DOT : '.';
FALSE : 'false';
FROM : 'from';
LP : '(';
NOT : 'not';
NULL : 'null';
OR : 'or';
ROW : 'row';
RP : ')';
PIPE : '|';
TRUE : 'true';
WHERE : 'where';

EQ  : '==';
NEQ : '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS : '+';
MINUS : '-';
ASTERISK : '*';
SLASH : '/';
PERCENT : '%';

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

QUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

LINE_COMMENT
    : '//' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

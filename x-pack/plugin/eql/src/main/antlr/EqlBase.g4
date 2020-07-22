/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

grammar EqlBase;


singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query pipe*
    ;

query
    : sequence
    | join
    | eventQuery
    ;

sequenceParams
    : WITH (MAXSPAN EQ timeUnit)
    ;

sequence
    : SEQUENCE (by=joinKeys sequenceParams? | sequenceParams disallowed=joinKeys?)?
      sequenceTerm sequenceTerm+
      (UNTIL until=sequenceTerm)?
    ;

join
    : JOIN (by=joinKeys)?
      joinTerm joinTerm+
      (UNTIL until=joinTerm)?
    ;

pipe
    : PIPE kind=IDENTIFIER (booleanExpression (COMMA booleanExpression)*)?
    ;


joinKeys
    : BY expression (COMMA expression)*
    ;

joinTerm
   : subquery (by=joinKeys)?
   ;

sequenceTerm
   : subquery (FORK (EQ booleanValue)?)? (by=joinKeys)?
   ;

subquery
    : LB eventFilter RB
    ;

eventQuery
    : eventFilter
    ;
    
eventFilter
    : (ANY | event=identifier) WHERE expression
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                               #logicalNot
    | relationship=IDENTIFIER OF subquery                                 #processCheck
    | valueExpression                                                     #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression         #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression          #logicalBinary
    ;


valueExpression
    : primaryExpression predicate?                                                      #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                     #comparison
    ;

// workaround for
//   https://github.com/antlr/antlr4/issues/780
//   https://github.com/antlr/antlr4/issues/781
predicate
    : NOT? kind=IN LP expression (COMMA expression)* RP
    ;

primaryExpression
    : constant                                                                          #constantDefault
    | functionExpression                                                                #function
    | qualifiedName                                                                     #dereference
    | LP expression RP                                                                  #parenthesizedExpression
    ;

functionExpression
    : name=IDENTIFIER LP (expression (COMMA expression)*)? RP
    ;

constant
    : NULL                                                                              #nullLiteral
    | number                                                                            #numericLiteral
    | booleanValue                                                                      #booleanLiteral
    | string                                                                            #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

qualifiedName
    : identifier (DOT identifier | LB INTEGER_VALUE+ RB)*
    ;

identifier
    : IDENTIFIER
    | ESCAPED_IDENTIFIER
    ;

timeUnit
    : number unit=IDENTIFIER?
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

string
    : STRING
    ;

AND: 'and';
ANY: 'any';
BY: 'by';
FALSE: 'false';
FORK: 'fork';
IN: 'in';
JOIN: 'join';
MAXSPAN: 'maxspan';
NOT: 'not';
NULL: 'null';
OF: 'of';
OR: 'or';
SEQUENCE: 'sequence';
TRUE: 'true';
UNTIL: 'until';
WHERE: 'where';
WITH: 'with';

// Operators
EQ  : '=' | '==';
NEQ : '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
DOT: '.';
COMMA: ',';
LB: '[';
RB: ']';
LP: '(';
RP: ')';
PIPE: '|';


ESCAPED_IDENTIFIER
    : '`' (~'`')* '`'
    ;

STRING
    : '\''  ('\\' [btnfr"'\\] | ~[\r\n'\\])* '\''
    | '"'   ('\\' [btnfr"'\\] | ~[\r\n"\\])* '"'
    | '?"'  ('\\"' |~["\r\n])* '"'
    | '?\'' ('\\\'' |~['\r\n])* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    | DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT
    ;

// make @timestamp not require escaping, since @ has no other meaning
IDENTIFIER
    : (LETTER | '_' | '@') (LETTER | DIGIT | '_')*
    ;

fragment EXPONENT
    : [Ee] [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
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


// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
/*
UNRECOGNIZED
    : .
    ;
*/

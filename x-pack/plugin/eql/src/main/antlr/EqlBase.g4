/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
    : WITH (MAXSPAN ASGN timeUnit)
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
   : subquery (by=joinKeys)?
   ;

subquery
    : LB eventFilter RB
    ;

eventQuery
    : eventFilter
    ;

eventFilter
    : (ANY | event=eventValue) WHERE expression
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
    : operatorExpression                                                                      #valueExpressionDefault
    | left=operatorExpression comparisonOperator right=operatorExpression                     #comparison
    ;

operatorExpression
    : primaryExpression predicate?                                                            #operatorExpressionDefault
    | operator=(MINUS | PLUS) operatorExpression                                              #arithmeticUnary
    | left=operatorExpression operator=(ASTERISK | SLASH | PERCENT) right=operatorExpression  #arithmeticBinary
    | left=operatorExpression operator=(PLUS | MINUS) right=operatorExpression                #arithmeticBinary
    ;

// workaround for
//   https://github.com/antlr/antlr4/issues/780
//   https://github.com/antlr/antlr4/issues/781
predicate
    : NOT? kind=(IN | IN_INSENSITIVE) LP expression (COMMA expression)* RP
    | kind=(SEQ | LIKE | LIKE_INSENSITIVE | REGEX | REGEX_INSENSITIVE) constant
    | kind=(SEQ | LIKE | LIKE_INSENSITIVE | REGEX | REGEX_INSENSITIVE) LP constant (COMMA constant)* RP
    ;

primaryExpression
    : constant                                                                          #constantDefault
    | functionExpression                                                                #function
    | qualifiedName                                                                     #dereference
    | LP expression RP                                                                  #parenthesizedExpression
    ;

functionExpression
    : name=functionName LP (expression (COMMA expression)*)? RP
    ;

functionName
    : IDENTIFIER
    | TILDE_IDENTIFIER
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
    | QUOTED_IDENTIFIER
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
IN: 'in';
IN_INSENSITIVE : 'in~';
JOIN: 'join';
LIKE: 'like';
LIKE_INSENSITIVE: 'like~';
MAXSPAN: 'maxspan';
NOT: 'not';
NULL: 'null';
OF: 'of';
OR: 'or';
REGEX: 'regex';
REGEX_INSENSITIVE: 'regex~';
SEQUENCE: 'sequence';
TRUE: 'true';
UNTIL: 'until';
WHERE: 'where';
WITH: 'with';

// Operators
// dedicated string equality - case-insensitive and supporting * operator
SEQ : ':';
// regular operators
ASGN : '=';
EQ  : '==';
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

fragment STRING_ESCAPE
    : '\\' [btnfr"'\\]
    ;

fragment HEX_DIGIT
    : [0-9abcdefABCDEF]
    ;

fragment UNICODE_ESCAPE
    : '\\u' '{' HEX_DIGIT+  '}' // 2-8 hex
    ;

fragment UNESCAPED_CHARS
    : ~[\r\n"\\]
    ;

STRING
    : '"' (STRING_ESCAPE | UNICODE_ESCAPE | UNESCAPED_CHARS)* '"'
    | '"""' (~[\r\n])*? '"""' '"'? '"'?
    // Old style quoting of string, handled as errors in AbstractBuilder
    | '\''  ('\\' [btnfr"'\\] | ~[\r\n'\\])* '\''
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

QUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

TILDE_IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')* '~'
    ;

eventValue
    : STRING
    | IDENTIFIER
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

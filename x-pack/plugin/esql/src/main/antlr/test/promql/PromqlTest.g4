/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
grammar PromqlTest;

singleExpression
    : expression EOF
    ;

// operator precedence defined in Promql at
// https://prometheus.io/docs/prometheus/latest/querying/operators/#binary-operator-precedence

expression
    : <assoc=right> left=expression op=CARET modifier? right=expression                         #arithmeticBinary
    | operator=(PLUS | MINUS) expression                                                        #arithmeticUnary
    | left=expression op=(ASTERISK | PERCENT | SLASH) modifier? right=expression                #arithmeticBinary
    | left=expression op=(MINUS | PLUS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=(EQ | NEQ | GT | GTE | LT | LTE) BOOL? modifier? right=expression      #arithmeticBinary
    | left=expression op=(AND | UNLESS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=OR modifier? right=expression                                          #arithmeticBinary
    | value                                                                                     #valueExpression
    | LP expression RP                                                                          #parenthesized
    | expression LSB range=duration subqueryResolution RSB evaluation?                            #subquery
    ;

subqueryResolution
    : COLON (resolution=duration)?
    | TIME_VALUE_WITH_COLON <assoc=right> op=CARET expression
    | TIME_VALUE_WITH_COLON op=(ASTERISK | SLASH) expression
    | TIME_VALUE_WITH_COLON op=(MINUS|PLUS) expression
    | TIME_VALUE_WITH_COLON
    ;

value
    : function
    | selector
    | constant
    ;

function
    : IDENTIFIER LP RP
    | IDENTIFIER LP expression (COMMA expression)* RP functionModifier?
    | IDENTIFIER functionModifier LP expression (COMMA expression)* RP
    ;

functionModifier
    : (BY | WITHOUT) labelList
    ;

selector
    : seriesMatcher (LSB duration RSB)? evaluation?
    ;

seriesMatcher
    : identifier (LCB labels? RCB)?
    | LCB labels RCB
    ;

modifier
    : (IGNORING | ON) modifierLabels=labelList (group=(GROUP_LEFT | GROUP_RIGHT) groupLabels=labelList?)?
    ;

// NB: PromQL explicitly allows a trailing comma for label enumeration
// both inside aggregation functions and metric labels.
labelList
    : LP (identifier COMMA?)* RP
    ;

labels
    :  label (COMMA label?)*
    ;

label
    : identifier kind=(LABEL_EQ | NEQ | LABEL_RGX | LABEL_RGX_NEQ) STRING
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

evaluation
    : offset at?
    | at offset?
    ;

offset
    : OFFSET MINUS? duration
    ;

// do timeunit validation and break-down inside the parser
// this helps deal with ambiguities for multi-unit declarations (1d3m)
// and give better error messages
// support arithmetic for duration with partial support added in Prometheus 3.4
// https://github.com/prometheus/prometheus/issues/12318
// https://github.com/prometheus/prometheus/pull/16249
duration
    //: time_value
    : expression
    ;
at
    : AT MINUS? number
    | AT (AT_START | AT_END)
    ;

constant
    : number
    | string
    | time_value
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    | HEXADECIMAL    #hexLiteral
    ;

string
    : STRING
    ;

time_value
    : TIME_VALUE_WITH_COLON
    | TIME_VALUE
    | number
    ;

// declared tokens that can be used without special escaping
// in PromQL this applies to all keywords
nonReserved
    : AND
    | BOOL
    | BY
    | GROUP_LEFT
    | GROUP_RIGHT
    | IGNORING
    | OFFSET
    | OR
    | ON
    | UNLESS
    | WITHOUT
    ;

// Operators

// math
PLUS    : '+';
MINUS   : '-';
ASTERISK: '*';
SLASH   : '/';
PERCENT : '%';
CARET   : '^';

// comparison
EQ : '==';
NEQ: '!=';
GT : '>';
GTE: '>=';
LT : '<';
LTE: '<=';

// Label
LABEL_EQ     : '=';
LABEL_RGX    : '=~';
LABEL_RGX_NEQ: '!~';

// set
AND   : 'and';
OR    : 'or';
UNLESS: 'unless';

// Modifiers

// aggregration
BY     : 'by';
WITHOUT: 'without';

// join
ON         : 'on';
IGNORING   : 'ignoring';
GROUP_LEFT : 'group_left';
GROUP_RIGHT: 'group_right';

// bool
BOOL: 'bool';

// evaluation
OFFSET  : 'offset' | 'OFFSET';  // the upper-case format seems to be a legacy construct
AT      : '@';
AT_START: 'start()';
AT_END  : 'end()';

// brackets
LCB: '{';
RCB: '}';
LSB: '[';
RSB: ']';
LP : '(';
RP : ')';

COLON: ':';
COMMA: ',';

STRING
    : SQ  ( '\\' [abfnrtv\\'] | ~'\'' )* SQ
    | DQ  ( '\\' [abfnrtv\\"] | ~'"'  )* DQ
    | '`'  ( ~'`' )* '`'
    ;

fragment ESC_CHARS
    : [abfnrtv\\]
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    | DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT
    | [iI][nN][fF]
    | [nN][aA][nN]
    ;

HEXADECIMAL
    : '0x'[0-9a-fA-F]+
    ;

//
// Special handling for time values to disambiguate from identifiers
//

// hack to allow colon as a time unit separator inside subquery duration to avoid the lexer picking it as an identifier
TIME_VALUE_WITH_COLON
    : COLON (DIGIT+ [a-zA-Z]+)+
    ;

// similar to the identifier but without a :
TIME_VALUE
    : (DIGIT+ [a-zA-Z]+)+
    ;

// NB: the parser needs to validates this token based on context
// (metric vs label vs..) as it can include non-supported characters
IDENTIFIER
    : [a-zA-Z_:][a-zA-Z0-9_:]*
    ;

COMMENT
    : '#' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
UNRECOGNIZED
    : .
    ;

fragment SQ
    : '\''
    ;

fragment DQ
    : '"'
    ;

fragment EXPONENT
    : [Ee] [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment DOT
    : '.'
    ;

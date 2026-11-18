/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar PromqlBaseLexer;

@header {
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
}

options {
  superClass=LexerConfig;
  caseInsensitive=true;
}

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
OFFSET  : 'offset';
AT      : '@';
AT_START: 'start' WS_CHAR* '(' WS_CHAR* ')';
AT_END  : 'end' WS_CHAR* '(' WS_CHAR* ')';

// brackets
LCB: '{';
RCB: '}';
LSB: '[' { incPromqlBracketDepth(); };
RSB: ']' { decPromqlBracketDepth(); };
LP : '(';
RP : ')';

COLON: ':';
COMMA: ',';

// a quoted string ends at its line; the parser validates each escape sequence
STRING
    : SQ  ( '\\' ~'\n' | ~['\\\n] )* SQ
    | DQ  ( '\\' ~'\n' | ~["\\\n] )* DQ
    | '`'  ( ~'`' )* '`'
    ;

// like Prometheus, a single underscore may separate digits, as in 1_000
INTEGER_VALUE
    : DIGITS
    ;

DECIMAL_VALUE
    : DIGITS DOT DIGITS?
    | DOT DIGITS
    | DIGITS (DOT DIGITS?)? EXPONENT
    | DOT DIGITS EXPONENT
    ;

// a number in expressions, but a name in label matchers, where Prometheus reads them as words
INF_OR_NAN
    : 'inf'
    | 'nan'
    ;

HEXADECIMAL
    : '0x' '_'? HEX_DIGIT ('_'? HEX_DIGIT)*
    ;

//
// Special handling for time values to disambiguate from identifiers
//

// similar to the identifier but without a :
TIME_VALUE
    : (DIGIT+ [a-z]+)+
    ;

// NB: the parser needs to validates this token based on context
// (metric vs label vs..) as it can include non-supported characters.
// Like Prometheus, a colon starts an identifier only outside brackets, as in :9, while inside them it separates a
// subquery range from its step, as in [5m:1m].
IDENTIFIER
    : [a-z_][a-z0-9_:.]*
    | {isInPromqlBrackets() == false}? ':' [a-z0-9_:][a-z0-9_:.]*
    ;

NAMED_OR_POSITIONAL_DOUBLE_PARAMS
    : PARAM_MARKER PARAM_MARKER (PARAM_LETTER | PARAM_UNDERSCORE) PARAM_ID_BODY*
    | PARAM_MARKER PARAM_MARKER DIGIT+
    ;

NAMED_OR_POSITIONAL_PARAM
    : PARAM_MARKER (PARAM_LETTER | PARAM_UNDERSCORE) PARAM_ID_BODY*
    | PARAM_MARKER DIGIT+
    ;

fragment PARAM_MARKER: '?';
fragment PARAM_LETTER: [a-z];
fragment PARAM_UNDERSCORE: '_';
fragment PARAM_ID_BODY: PARAM_LETTER | DIGIT | PARAM_UNDERSCORE;

COMMENT
    : '#' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

WS
    : WS_CHAR+ -> channel(HIDDEN)
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
    : 'e' [+-]? DIGITS
    ;

fragment DIGITS
    : DIGIT ('_'? DIGIT)*
    ;

fragment DIGIT
    : [0-9]
    ;

fragment HEX_DIGIT
    : [0-9a-f]
    ;

fragment WS_CHAR
    : [ \r\n\t]
    ;

fragment DOT
    : '.'
    ;

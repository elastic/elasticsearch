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
    | 'inf'
    | 'nan'
    ;

HEXADECIMAL
    : '0x'[0-9a-f]+
    ;

//
// Special handling for time values to disambiguate from identifiers
//

// hack to allow colon as a time unit separator inside subquery duration to avoid the lexer picking it as an identifier
TIME_VALUE_WITH_COLON
    : COLON (DIGIT+ [a-z]+)+
    ;

// similar to the identifier but without a :
TIME_VALUE
    : (DIGIT+ [a-z]+)+
    ;

// NB: the parser needs to validates this token based on context
// (metric vs label vs..) as it can include non-supported characters
IDENTIFIER
    : [a-z_:][a-z0-9_:.]*
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
    : 'e' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment DOT
    : '.'
    ;

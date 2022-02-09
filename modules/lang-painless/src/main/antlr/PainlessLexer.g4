/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

lexer grammar PainlessLexer;

@members {
/** Is the preceding {@code /} a the beginning of a regex (true) or a division (false). */
protected abstract boolean isSlashRegex();
}

WS: [ \t\n\r]+ -> skip;
COMMENT: ( '//' .*? [\n\r] | '/*' .*? '*/' ) -> skip;

LBRACK:    '{';
RBRACK:    '}';
LBRACE:    '[';
RBRACE:    ']';
LP:        '(';
RP:        ')';
DOLLAR:    '$';
// We switch modes after a dot to ensure there are not conflicts
// between shortcuts and decimal values.  Without the mode switch
// shortcuts such as id.0.0 will fail because 0.0 will be interpreted
// as a decimal value instead of two individual list-style shortcuts.
DOT:       '.'  -> mode(AFTER_DOT);
NSDOT:     '?.' -> mode(AFTER_DOT);
COMMA:     ',';
SEMICOLON: ';';
IF:        'if';
IN:        'in';
ELSE:      'else';
WHILE:     'while';
DO:        'do';
FOR:       'for';
CONTINUE:  'continue';
BREAK:     'break';
RETURN:    'return';
NEW:       'new';
TRY:       'try';
CATCH:     'catch';
THROW:     'throw';
THIS:      'this';
INSTANCEOF: 'instanceof';

BOOLNOT: '!';
BWNOT:   '~';
MUL:     '*';
DIV:     '/' { isSlashRegex() == false }?;
REM:     '%';
ADD:     '+';
SUB:     '-';
LSH:     '<<';
RSH:     '>>';
USH:     '>>>';
LT:      '<';
LTE:     '<=';
GT:      '>';
GTE:     '>=';
EQ:      '==';
EQR:     '===';
NE:      '!=';
NER:     '!==';
BWAND:   '&';
XOR:     '^';
BWOR:    '|';
BOOLAND: '&&';
BOOLOR:  '||';
COND:    '?';
COLON:   ':';
ELVIS:   '?:';
REF:     '::';
ARROW:   '->';
FIND:    '=~';
MATCH:   '==~';
INCR:    '++';
DECR:    '--';

ASSIGN: '=';
AADD:   '+=';
ASUB:   '-=';
AMUL:   '*=';
ADIV:   '/=';
AREM:   '%=';
AAND:   '&=';
AXOR:   '^=';
AOR:    '|=';
ALSH:   '<<=';
ARSH:   '>>=';
AUSH:   '>>>=';

OCTAL: '0' [0-7]+ [lL]?;
HEX: '0' [xX] [0-9a-fA-F]+ [lL]?;
INTEGER: ( '0' | [1-9] [0-9]* ) [lLfFdD]?;
DECIMAL: ( '0' | [1-9] [0-9]* ) (DOT [0-9]+)? ( [eE] [+\-]? [0-9]+ )? [fFdD]?;

STRING: ( '"' ( '\\"' | '\\\\' | ~[\\"] )*? '"' ) | ( '\'' ( '\\\'' | '\\\\' | ~[\\'] )*? '\'' );
REGEX: '/' ( '\\' ~'\n' | ~('/' | '\n') )+? '/' [cilmsUux]* { isSlashRegex() }?;

TRUE:  'true';
FALSE: 'false';

NULL: 'null';

PRIMITIVE: 'boolean' | 'byte' | 'short' | 'char' | 'int' | 'long' | 'float' | 'double';
DEF: 'def';

ID: [_a-zA-Z] [_a-zA-Z0-9]*;

mode AFTER_DOT;

DOTINTEGER: ( '0' | [1-9] [0-9]* ) -> mode(DEFAULT_MODE);
DOTID: [_a-zA-Z] [_a-zA-Z0-9]*     -> mode(DEFAULT_MODE);

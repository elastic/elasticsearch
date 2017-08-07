/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

lexer grammar PainlessLexer;

@members{
/**
 * Check against the current whitelist to determine whether a token is a type
 * or not. Called by the {@code TYPE} token defined in {@code PainlessLexer.g4}.
 * See also
 * <a href="https://en.wikipedia.org/wiki/The_lexer_hack">The lexer hack</a>.
 */
protected abstract boolean isSimpleType(String name);

/**
 * Is the preceding {@code /} a the beginning of a regex (true) or a division
 * (false).
 */
protected abstract boolean slashIsRegex();
}

WS: [ \t\n\r]+ -> skip;
COMMENT: ( '//' .*? [\n\r] | '/*' .*? '*/' ) -> skip;

LBRACK:    '{';
RBRACK:    '}';
LBRACE:    '[';
RBRACE:    ']';
LP:        '(';
RP:        ')';
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
DIV:     '/' { false == slashIsRegex() }?;
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
REGEX: '/' ( '\\' ~'\n' | ~('/' | '\n') )+? '/' [cilmsUux]* { slashIsRegex() }?;

TRUE:  'true';
FALSE: 'false';

NULL: 'null';

// The predicate here allows us to remove ambiguities when
// dealing with types versus identifiers.  We check against
// the current whitelist to determine whether a token is a type
// or not.  Note this works by processing one character at a time
// and the rule is added or removed as this happens.  This is also known
// as "the lexer hack."  See (https://en.wikipedia.org/wiki/The_lexer_hack).
TYPE: ID ( DOT ID )* { isSimpleType(getText()) }?;
ID: [_a-zA-Z] [_a-zA-Z0-9]*;

mode AFTER_DOT;

DOTINTEGER: ( '0' | [1-9] [0-9]* )                        -> mode(DEFAULT_MODE);
DOTID: [_a-zA-Z] [_a-zA-Z0-9]*                            -> mode(DEFAULT_MODE);

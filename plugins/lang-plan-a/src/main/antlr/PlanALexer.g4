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

lexer grammar PlanALexer;

@header {
    import java.util.Set;
}

@members {
    private Set<String> types = null;

    void setTypes(Set<String> types) {
        this.types = types;
    }
}

WS: [ \t\n\r]+ -> skip;
COMMENT: ( '//' .*? [\n\r] | '/*' .*? '*/' ) -> skip;

LBRACK:    '{';
RBRACK:    '}';
LBRACE:    '[';
RBRACE:    ']';
LP:        '(';
RP:        ')';
DOT:       '.' -> mode(EXT);
COMMA:     ',';
SEMICOLON: ';';
IF:        'if';
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

BOOLNOT: '!';
BWNOT:   '~';
MUL:     '*';
DIV:     '/';
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
BWXOR:   '^';
BWOR:    '|';
BOOLAND: '&&';
BOOLOR:  '||';
COND:    '?';
COLON:   ':';
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
ACAT:   '..=';

OCTAL: '0' [0-7]+ [lL]?;
HEX: '0' [xX] [0-9a-fA-F]+ [lL]?;
INTEGER: ( '0' | [1-9] [0-9]* ) [lLfFdD]?;
DECIMAL: ( '0' | [1-9] [0-9]* ) DOT [0-9]* ( [eE] [+\-]? [0-9]+ )? [fF]?;

STRING: '"' ( '\\"' | '\\\\' | ~[\\"] )*? '"' {setText(getText().substring(1, getText().length() - 1));};
CHAR: '\'' . '\''                             {setText(getText().substring(1, getText().length() - 1));};

TRUE:  'true';
FALSE: 'false';

NULL: 'null';

TYPE: ID GENERIC? {types.contains(getText().replace(" ", ""))}? {setText(getText().replace(" ", ""));};
fragment GENERIC: ' '* '<' ' '* ( ID GENERIC? ) ' '* ( COMMA ' '* ( ID GENERIC? ) ' '* )* '>';
ID: [_a-zA-Z] [_a-zA-Z0-9]*;

mode EXT;
EXTINTEGER: ( '0' | [1-9] [0-9]* ) -> mode(DEFAULT_MODE);
EXTID: [_a-zA-Z] [_a-zA-Z0-9]* -> mode(DEFAULT_MODE);

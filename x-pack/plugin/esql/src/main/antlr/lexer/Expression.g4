/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
lexer grammar Expression;

//
// Expression - used by many commands
//
COMPLETION : 'completion'     -> pushMode(EXPRESSION_MODE);
DISSECT : 'dissect'           -> pushMode(EXPRESSION_MODE);
EVAL : 'eval'                 -> pushMode(EXPRESSION_MODE);
GROK : 'grok'                 -> pushMode(EXPRESSION_MODE);
LIMIT : 'limit'               -> pushMode(EXPRESSION_MODE);
ROW : 'row'                   -> pushMode(EXPRESSION_MODE);
SORT : 'sort'                 -> pushMode(EXPRESSION_MODE);
STATS : 'stats'               -> pushMode(EXPRESSION_MODE);
WHERE : 'where'               -> pushMode(EXPRESSION_MODE);

DEV_INLINESTATS : {this.isDevVersion()}? 'inlinestats' -> pushMode(EXPRESSION_MODE);
DEV_RERANK : {this.isDevVersion()}? 'rerank'           -> pushMode(EXPRESSION_MODE);
DEV_SAMPLE : {this.isDevVersion()}? 'sample'           -> pushMode(EXPRESSION_MODE);


mode EXPRESSION_MODE;

PIPE : '|' -> popMode;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-z]
    ;

fragment ESCAPE_SEQUENCE
    : '\\' [tnr"\\]
    ;

fragment UNESCAPED_CHARS
    : ~[\r\n"\\]
    ;

fragment EXPONENT
    : [e] [+-]? DIGIT+
    ;

fragment ASPERAND
    : '@'
    ;

fragment BACKQUOTE
    : '`'
    ;

fragment BACKQUOTE_BLOCK
    : ~'`'
    | '``'
    ;

fragment UNDERSCORE
    : '_'
    ;

fragment UNQUOTED_ID_BODY
    : (LETTER | DIGIT | UNDERSCORE)
    ;

QUOTED_STRING
    : '"' (ESCAPE_SEQUENCE | UNESCAPED_CHARS)* '"'
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
AS: 'as';
ASC : 'asc';
ASSIGN : '=';
BY : 'by';
CAST_OP : '::';
COLON : ':';
COMMA : ',';
DESC : 'desc';
DOT : '.';
FALSE : 'false';
FIRST : 'first';
IN: 'in';
IS: 'is';
LAST : 'last';
LIKE: 'like';
NOT : 'not';
NULL : 'null';
NULLS : 'nulls';
ON: 'on';
OR : 'or';
PARAM: '?';
RLIKE: 'rlike';
TRUE : 'true';
WITH: 'with';

EQ  : '==';
CIEQ  : '=~';
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

LEFT_BRACES : '{';
RIGHT_BRACES : '}';

DOUBLE_PARAMS: '??';

NESTED_WHERE : WHERE -> type(WHERE);

NAMED_OR_POSITIONAL_PARAM
    : PARAM (LETTER | UNDERSCORE) UNQUOTED_ID_BODY*
    | PARAM DIGIT+
    ;

NAMED_OR_POSITIONAL_DOUBLE_PARAMS
    : DOUBLE_PARAMS (LETTER | UNDERSCORE) UNQUOTED_ID_BODY*
    | DOUBLE_PARAMS DIGIT+
    ;

// Brackets are funny. We can happen upon a CLOSING_BRACKET in two ways - one
// way is to start in an explain command which then shifts us to expression
// mode. Thus, the two popModes on CLOSING_BRACKET. The other way could as
// the start of a multivalued field constant. To line up with the double pop
// the explain mode needs, we double push when we see that.
OPENING_BRACKET : '[' -> pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);
CLOSING_BRACKET : ']' -> popMode, popMode;

LP : '(' -> pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);
RP : ')' -> popMode, popMode;

UNQUOTED_IDENTIFIER
    : LETTER UNQUOTED_ID_BODY*
    // only allow @ at beginning of identifier to keep the option to allow @ as infix operator in the future
    // also, single `_` and `@` characters are not valid identifiers
    | (UNDERSCORE | ASPERAND) UNQUOTED_ID_BODY+
    ;

fragment QUOTED_ID
    : BACKQUOTE BACKQUOTE_BLOCK+ BACKQUOTE
    ;

QUOTED_IDENTIFIER
    : QUOTED_ID
    ;

EXPR_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

EXPR_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

EXPR_WS
    : WS -> channel(HIDDEN)
    ;

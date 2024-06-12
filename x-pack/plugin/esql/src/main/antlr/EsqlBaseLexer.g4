/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

lexer grammar EsqlBaseLexer;

DISSECT : 'dissect'           -> pushMode(EXPRESSION_MODE);
DROP : 'drop'                 -> pushMode(PROJECT_MODE);
ENRICH : 'enrich'             -> pushMode(ENRICH_MODE);
EVAL : 'eval'                 -> pushMode(EXPRESSION_MODE);
EXPLAIN : 'explain'           -> pushMode(EXPLAIN_MODE);
FROM : 'from'                 -> pushMode(FROM_MODE);
GROK : 'grok'                 -> pushMode(EXPRESSION_MODE);
INLINESTATS : 'inlinestats'   -> pushMode(EXPRESSION_MODE);
KEEP : 'keep'                 -> pushMode(PROJECT_MODE);
LIMIT : 'limit'               -> pushMode(EXPRESSION_MODE);
LOOKUP : 'lookup'             -> pushMode(LOOKUP_MODE);
META : 'meta'                 -> pushMode(META_MODE);
METRICS : 'metrics'           -> pushMode(METRICS_MODE);
MV_EXPAND : 'mv_expand'       -> pushMode(MVEXPAND_MODE);
RENAME : 'rename'             -> pushMode(RENAME_MODE);
ROW : 'row'                   -> pushMode(EXPRESSION_MODE);
SEARCH : 'search'             -> pushMode(SEARCH_MODE);
SHOW : 'show'                 -> pushMode(SHOW_MODE);
SORT : 'sort'                 -> pushMode(EXPRESSION_MODE);
STATS : 'stats'               -> pushMode(EXPRESSION_MODE);
WHERE : 'where'               -> pushMode(EXPRESSION_MODE);
UNKNOWN_CMD : ~[ \r\n\t[\]/]+ -> pushMode(EXPRESSION_MODE);

LINE_COMMENT
    : '//' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' (MULTILINE_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

fragment INDEX_UNQUOTED_IDENTIFIER_PART
    : ~[=`|,[\]/ \t\r\n]
    | '/' ~[*/] // allow single / but not followed by another / or * which would start a comment
    ;

INDEX_UNQUOTED_IDENTIFIER
    : INDEX_UNQUOTED_IDENTIFIER_PART+
    ;

//
// Explain
//
mode EXPLAIN_MODE;
EXPLAIN_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(DEFAULT_MODE);
EXPLAIN_PIPE : PIPE -> type(PIPE), popMode;
EXPLAIN_WS : WS -> channel(HIDDEN);
EXPLAIN_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
EXPLAIN_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);

//
// Expression - used by most command
//
mode EXPRESSION_MODE;

PIPE : '|' -> popMode;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Za-z]
    ;

fragment ESCAPE_SEQUENCE
    : '\\' [tnr"\\]
    ;

fragment UNESCAPED_CHARS
    : ~[\r\n"\\]
    ;

fragment EXPONENT
    : [Ee] [+-]? DIGIT+
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

BY : 'by';

AND : 'and';
ASC : 'asc';
ASSIGN : '=';
CAST_OP : '::';
COMMA : ',';
DESC : 'desc';
DOT : '.';
FALSE : 'false';
FIRST : 'first';
LAST : 'last';
LP : '(';
IN: 'in';
IS: 'is';
LIKE: 'like';
NOT : 'not';
NULL : 'null';
NULLS : 'nulls';
OR : 'or';
PARAM: '?';
RLIKE: 'rlike';
RP : ')';
TRUE : 'true';

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

NAMED_OR_POSITIONAL_PARAM
    : PARAM LETTER UNQUOTED_ID_BODY*
    | PARAM DIGIT+
    ;

// Brackets are funny. We can happen upon a CLOSING_BRACKET in two ways - one
// way is to start in an explain command which then shifts us to expression
// mode. Thus, the two popModes on CLOSING_BRACKET. The other way could as
// the start of a multivalued field constant. To line up with the double pop
// the explain mode needs, we double push when we see that.
OPENING_BRACKET : '[' -> pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);
CLOSING_BRACKET : ']' -> popMode, popMode;

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

//
// Search Expression - mostly a copy of EXPRESSION_MODE + match
//
mode SEARCH_EXPRESSION_MODE;

SEARCH_EXPR_PIPE : PIPE -> type(PIPE), popMode;
SEARCH_EXPR_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);
SEARCH_EXPR_INTEGER_LITERAL : INTEGER_LITERAL -> type(INTEGER_LITERAL);
SEARCH_EXPR_DECIMAL_LITERAL : DECIMAL_LITERAL -> type(DECIMAL_LITERAL);

SEARCH_EXPR_BY : BY -> type(BY);

SEARCH_EXPR_AND : AND -> type(AND);
SEARCH_EXPR_ASC : ASC -> type(ASC);
SEARCH_EXPR_ASSIGN : ASSIGN -> type(ASSIGN);
SEARCH_EXPR_CAST_OP : CAST_OP -> type(CAST_OP);
SEARCH_EXPR_COMMA : COMMA -> type(COMMA);
SEARCH_EXPR_DESC : DESC -> type(DESC);
SEARCH_EXPR_DOT : DOT -> type(DOT);
SEARCH_EXPR_FALSE : FALSE -> type(FALSE);
SEARCH_EXPR_FIRST : FIRST -> type(FIRST);
SEARCH_EXPR_LAST : LAST -> type(LAST);
SEARCH_EXPR_LP : LP -> type(LP);
SEARCH_EXPR_IN : IN -> type(IN);
SEARCH_EXPR_IS : IS -> type(IS);
SEARCH_EXPR_LIKE : LIKE -> type(LIKE);
SEARCH_EXPR_NOT : NOT -> type(NOT);
SEARCH_EXPR_NULL : NULL -> type(NULL);
SEARCH_EXPR_NULLS : NULLS -> type(NULLS);
SEARCH_EXPR_OR : OR -> type(OR);
SEARCH_EXPR_PARAM: PARAM -> type(PARAM);
SEARCH_EXPR_RLIKE : RLIKE -> type(RLIKE);
SEARCH_EXPR_RP : RP -> type(RP);
SEARCH_EXPR_TRUE : TRUE -> type(TRUE);

SEARCH_EXPR_EQ    : EQ   -> type(EQ);
SEARCH_EXPR_CIEQ  : CIEQ -> type(CIEQ);
SEARCH_EXPR_NEQ   : NEQ  -> type(NEQ);
SEARCH_EXPR_LT    : LT   -> type(LT);
SEARCH_EXPR_LTE   : LTE  -> type(LTE);
SEARCH_EXPR_GT    : GT   -> type(GT);
SEARCH_EXPR_GTE   : GTE  -> type(GTE);

SEARCH_EXPR_PLUS     : PLUS     -> type(PLUS);
SEARCH_EXPR_MINUS    : MINUS    -> type(MINUS);
SEARCH_EXPR_ASTERISK : ASTERISK -> type(ASTERISK);
SEARCH_EXPR_SLASH    : SLASH    -> type(SLASH);
SEARCH_EXPR_PERCENT  : PERCENT  -> type(PERCENT);

SEARCH_EXPR_MATCH : 'match';
SEARCH_EXPR_MATCH_PHRASE : 'match phrase';

SEARCH_EXPR_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(EXPRESSION_MODE), pushMode(EXPRESSION_MODE);
SEARCH_EXPR_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET), popMode, popMode;

SEARCH_EXPR_UNQUOTED_IDENTIFIER : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER);
SEARCH_EXPR_QUOTED_IDENTIFIER   : QUOTED_IDENTIFIER   -> type(QUOTED_IDENTIFIER);

SEARCH_EXPR_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_EXPR_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_EXPR_WS
    : WS -> channel(HIDDEN)
    ;

//
// FROM command
//
mode FROM_MODE;
FROM_PIPE : PIPE -> type(PIPE), popMode;
FROM_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET);
FROM_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET);
FROM_COMMA : COMMA -> type(COMMA);
FROM_ASSIGN : ASSIGN -> type(ASSIGN);
FROM_QUOTED_STRING : QUOTED_STRING -> type(QUOTED_STRING);

METADATA : 'metadata';

FROM_INDEX_UNQUOTED_IDENTIFIER
    : INDEX_UNQUOTED_IDENTIFIER -> type(INDEX_UNQUOTED_IDENTIFIER)
    ;

FROM_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

FROM_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

FROM_WS
    : WS -> channel(HIDDEN)
    ;
//
// DROP, KEEP
//
mode PROJECT_MODE;
PROJECT_PIPE : PIPE -> type(PIPE), popMode;
PROJECT_DOT: DOT -> type(DOT);
PROJECT_COMMA : COMMA -> type(COMMA);

fragment UNQUOTED_ID_BODY_WITH_PATTERN
    : (LETTER | DIGIT | UNDERSCORE | ASTERISK)
    ;

fragment UNQUOTED_ID_PATTERN
    : (LETTER | ASTERISK) UNQUOTED_ID_BODY_WITH_PATTERN*
    | (UNDERSCORE | ASPERAND) UNQUOTED_ID_BODY_WITH_PATTERN+
    ;

ID_PATTERN
    : (UNQUOTED_ID_PATTERN | QUOTED_ID)+
    ;

PROJECT_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

PROJECT_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

PROJECT_WS
    : WS -> channel(HIDDEN)
    ;
//
// | RENAME a.b AS x, c AS y
//
mode RENAME_MODE;
RENAME_PIPE : PIPE -> type(PIPE), popMode;
RENAME_ASSIGN : ASSIGN -> type(ASSIGN);
RENAME_COMMA : COMMA -> type(COMMA);
RENAME_DOT: DOT -> type(DOT);

AS : 'as';

RENAME_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

RENAME_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

RENAME_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

RENAME_WS
    : WS -> channel(HIDDEN)
    ;

// | ENRICH ON key WITH fields
mode ENRICH_MODE;
ENRICH_PIPE : PIPE -> type(PIPE), popMode;
ENRICH_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(SETTING_MODE);

ON : 'on'     -> pushMode(ENRICH_FIELD_MODE);
WITH : 'with' -> pushMode(ENRICH_FIELD_MODE);

// similar to that of an index
// see https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
fragment ENRICH_POLICY_NAME_BODY
    : ~[\\/?"<>| ,#\t\r\n:]
    ;

ENRICH_POLICY_NAME
    // allow prefix for the policy to specify its resolution
    : (ENRICH_POLICY_NAME_BODY+ COLON)? ENRICH_POLICY_NAME_BODY+
    ;

ENRICH_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

ENRICH_MODE_UNQUOTED_VALUE
    : ENRICH_POLICY_NAME -> type(ENRICH_POLICY_NAME)
    ;

ENRICH_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_WS
    : WS -> channel(HIDDEN)
    ;

// submode for Enrich to allow different lexing between policy identifier (loose) and field identifiers
mode ENRICH_FIELD_MODE;
ENRICH_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
ENRICH_FIELD_ASSIGN : ASSIGN -> type(ASSIGN);
ENRICH_FIELD_COMMA : COMMA -> type(COMMA);
ENRICH_FIELD_DOT: DOT -> type(DOT);

ENRICH_FIELD_WITH : WITH -> type(WITH) ;

ENRICH_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

ENRICH_FIELD_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

ENRICH_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

ENRICH_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

// LOOKUP ON key
mode LOOKUP_MODE;
LOOKUP_PIPE : PIPE -> type(PIPE), popMode;
LOOKUP_COMMA : COMMA -> type(COMMA);
LOOKUP_DOT: DOT -> type(DOT);
LOOKUP_ON : ON -> type(ON), pushMode(LOOKUP_FIELD_MODE);

LOOKUP_INDEX_UNQUOTED_IDENTIFIER
    : INDEX_UNQUOTED_IDENTIFIER -> type(INDEX_UNQUOTED_IDENTIFIER)
    ;

LOOKUP_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_WS
    : WS -> channel(HIDDEN)
    ;

mode LOOKUP_FIELD_MODE;
LOOKUP_FIELD_PIPE : PIPE -> type(PIPE), popMode, popMode;
LOOKUP_FIELD_COMMA : COMMA -> type(COMMA);
LOOKUP_FIELD_DOT: DOT -> type(DOT);

LOOKUP_FIELD_ID_PATTERN
    : ID_PATTERN -> type(ID_PATTERN)
    ;

LOOKUP_FIELD_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_FIELD_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

LOOKUP_FIELD_WS
    : WS -> channel(HIDDEN)
    ;

mode MVEXPAND_MODE;
MVEXPAND_PIPE : PIPE -> type(PIPE), popMode;
MVEXPAND_DOT: DOT -> type(DOT);

MVEXPAND_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

MVEXPAND_UNQUOTED_IDENTIFIER
    : UNQUOTED_IDENTIFIER -> type(UNQUOTED_IDENTIFIER)
    ;

MVEXPAND_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

MVEXPAND_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

MVEXPAND_WS
    : WS -> channel(HIDDEN)
    ;

//
// SHOW commands
//
mode SHOW_MODE;
SHOW_PIPE : PIPE -> type(PIPE), popMode;

INFO : 'info';

SHOW_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_WS
    : WS -> channel(HIDDEN)
    ;

//
// META commands
//
mode META_MODE;
META_PIPE : PIPE -> type(PIPE), popMode;

FUNCTIONS : 'functions';

META_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

META_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

META_WS
    : WS -> channel(HIDDEN)
    ;

mode SETTING_MODE;
SETTING_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET), popMode;

COLON : ':';

SETTING
    : (ASPERAND | DIGIT| DOT | LETTER | UNDERSCORE)+
    ;

SETTING_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SETTTING_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SETTING_WS
    : WS -> channel(HIDDEN)
    ;


//
// METRICS command
//
mode METRICS_MODE;
METRICS_PIPE : PIPE -> type(PIPE), popMode;

METRICS_INDEX_UNQUOTED_IDENTIFIER
    : INDEX_UNQUOTED_IDENTIFIER -> type(INDEX_UNQUOTED_IDENTIFIER), popMode, pushMode(CLOSING_METRICS_MODE)
    ;

METRICS_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

METRICS_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

METRICS_WS
    : WS -> channel(HIDDEN)
    ;

// TODO: remove this workaround mode - see https://github.com/elastic/elasticsearch/issues/108528
mode CLOSING_METRICS_MODE;

CLOSING_METRICS_COMMA
    : COMMA -> type(COMMA), popMode, pushMode(METRICS_MODE)
    ;

CLOSING_METRICS_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

CLOSING_METRICS_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

CLOSING_METRICS_WS
    : WS -> channel(HIDDEN)
    ;

CLOSING_METRICS_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> popMode, pushMode(EXPRESSION_MODE), type(QUOTED_IDENTIFIER)
    ;

CLOSING_METRICS_UNQUOTED_IDENTIFIER
    :UNQUOTED_IDENTIFIER -> popMode, pushMode(EXPRESSION_MODE), type(UNQUOTED_IDENTIFIER)
    ;

CLOSING_METRICS_BY
    :BY -> popMode, pushMode(EXPRESSION_MODE), type(BY)
    ;

CLOSING_METRICS_PIPE
    : PIPE -> type(PIPE), popMode
    ;
//
// SEARCH command
//
mode SEARCH_MODE;
SEARCH_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET), pushMode(SEARCH_CTX_MODE);
// closing bracket is declared in case of empty declaration
SEARCH_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET);
// close current mode (go to DEFAULT_MODE)
SEARCH_PIPE: PIPE -> type(PIPE), popMode;
SEARCH_COMMA: COMMA -> type(COMMA);

SEARCH_UNQUOTED_IDENTIFIER
    : INDEX_UNQUOTED_IDENTIFIER -> type(INDEX_UNQUOTED_IDENTIFIER)
    ;

SEARCH_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_WS
    : WS -> channel(HIDDEN)
    ;

mode SEARCH_CTX_MODE;
// go back to SEARCH_MODE
SEARCH_CTX_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET), popMode;
// eat first pipe
SEARCH_CTX_PIPE: PIPE -> type(PIPE);

// available sub-commands
// NB: expression mode will exit on ] by doing two pops (1. to SEARCH_CTX_MODE, 2. to SEARCH_MODE)
SEARCH_CTX_LIMIT : LIMIT  -> type(LIMIT), pushMode(SEARCH_EXPRESSION_MODE);
SEARCH_CTX_SORT  : SORT   -> type(SORT),  pushMode(SEARCH_EXPRESSION_MODE);
RANK             : 'rank' -> pushMode(SEARCH_EXPRESSION_MODE);
SEARCH_CTX_WHERE : WHERE  -> type(WHERE), pushMode(SEARCH_EXPRESSION_MODE);

SEARCH_CTX_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_CTX_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SEARCH_CTX_WS
    : WS -> channel(HIDDEN)
    ;

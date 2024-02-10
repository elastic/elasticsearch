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
MV_EXPAND : 'mv_expand'       -> pushMode(MVEXPAND_MODE);
RENAME : 'rename'             -> pushMode(RENAME_MODE);
ROW : 'row'                   -> pushMode(EXPRESSION_MODE);
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

STRING
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

QUOTED_IDENTIFIER
    : BACKQUOTE BACKQUOTE_BLOCK+ BACKQUOTE
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
// FROM command
//
mode FROM_MODE;
FROM_PIPE : PIPE -> type(PIPE), popMode;
FROM_OPENING_BRACKET : OPENING_BRACKET -> type(OPENING_BRACKET);
FROM_CLOSING_BRACKET : CLOSING_BRACKET -> type(CLOSING_BRACKET);
FROM_COMMA : COMMA -> type(COMMA);
FROM_ASSIGN : ASSIGN -> type(ASSIGN);

METADATA: 'metadata';

fragment FROM_UNQUOTED_IDENTIFIER_PART
    : ~[=`|,[\]/ \t\r\n]
    | '/' ~[*/] // allow single / but not followed by another / or * which would start a comment
    ;

FROM_UNQUOTED_IDENTIFIER
    : FROM_UNQUOTED_IDENTIFIER_PART+
    ;

FROM_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
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

UNQUOTED_ID_PATTERN
    : (LETTER | ASTERISK) UNQUOTED_ID_BODY_WITH_PATTERN*
    | (UNDERSCORE | ASPERAND) UNQUOTED_ID_BODY_WITH_PATTERN+
    ;

PROJECT_UNQUOTED_IDENTIFIER
    : UNQUOTED_ID_PATTERN -> type(UNQUOTED_ID_PATTERN)
    ;

PROJECT_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
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

RENAME_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER -> type(QUOTED_IDENTIFIER)
    ;

// use the unquoted pattern to let the parser invalidate fields with *
RENAME_UNQUOTED_IDENTIFIER
    : UNQUOTED_ID_PATTERN -> type(UNQUOTED_ID_PATTERN)
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

ENRICH_FIELD_UNQUOTED_IDENTIFIER
    : UNQUOTED_ID_PATTERN -> type(UNQUOTED_ID_PATTERN)
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
// SHOW INFO
//
mode SHOW_MODE;
SHOW_PIPE : PIPE -> type(PIPE), popMode;

INFO : 'info';
FUNCTIONS : 'functions';

SHOW_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SHOW_WS
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


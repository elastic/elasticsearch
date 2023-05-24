lexer grammar EsqlBaseLexer;

DISSECT : 'dissect' -> pushMode(EXPRESSION);
DROP : 'drop' -> pushMode(SOURCE_IDENTIFIERS);
ENRICH : 'enrich' -> pushMode(SOURCE_IDENTIFIERS);
EVAL : 'eval' -> pushMode(EXPRESSION);
EXPLAIN : 'explain' -> pushMode(EXPLAIN_MODE);
FROM : 'from' -> pushMode(SOURCE_IDENTIFIERS);
GROK : 'grok' -> pushMode(EXPRESSION);
INLINESTATS : 'inlinestats' -> pushMode(EXPRESSION);
LIMIT : 'limit' -> pushMode(EXPRESSION);
MV_EXPAND : 'mv_expand' -> pushMode(SOURCE_IDENTIFIERS);
PROJECT : 'project' -> pushMode(SOURCE_IDENTIFIERS);
RENAME : 'rename' -> pushMode(SOURCE_IDENTIFIERS);
ROW : 'row' -> pushMode(EXPRESSION);
SHOW : 'show' -> pushMode(EXPRESSION);
SORT : 'sort' -> pushMode(EXPRESSION);
STATS : 'stats' -> pushMode(EXPRESSION);
WHERE : 'where' -> pushMode(EXPRESSION);
UNKNOWN_CMD : ~[ \r\n\t[\]/]+ -> pushMode(EXPRESSION);

LINE_COMMENT
    : '//' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

MULTILINE_COMMENT
    : '/*' (MULTILINE_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;


mode EXPLAIN_MODE;
EXPLAIN_OPENING_BRACKET : '[' -> type(OPENING_BRACKET), pushMode(DEFAULT_MODE);
EXPLAIN_PIPE : '|' -> type(PIPE), popMode;
EXPLAIN_WS : WS -> channel(HIDDEN);
EXPLAIN_LINE_COMMENT : LINE_COMMENT -> channel(HIDDEN);
EXPLAIN_MULTILINE_COMMENT : MULTILINE_COMMENT -> channel(HIDDEN);

mode EXPRESSION;

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
LIKE: 'like';
NOT : 'not';
NULL : 'null';
NULLS : 'nulls';
OR : 'or';
RLIKE: 'rlike';
RP : ')';
TRUE : 'true';
INFO : 'info';
FUNCTIONS : 'functions';

EQ  : '==';
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
OPENING_BRACKET : '[' -> pushMode(EXPRESSION), pushMode(EXPRESSION);
CLOSING_BRACKET : ']' -> popMode, popMode;


UNQUOTED_IDENTIFIER
    : LETTER (LETTER | DIGIT | '_')*
    // only allow @ at beginning of identifier to keep the option to allow @ as infix operator in the future
    // also, single `_` and `@` characters are not valid identifiers
    | ('_' | '@') (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
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



mode SOURCE_IDENTIFIERS;

SRC_PIPE : '|' -> type(PIPE), popMode;
SRC_CLOSING_BRACKET : ']' -> popMode, popMode, type(CLOSING_BRACKET);
SRC_COMMA : ',' -> type(COMMA);
SRC_ASSIGN : '=' -> type(ASSIGN);
ON : 'on';

SRC_UNQUOTED_IDENTIFIER
    : SRC_UNQUOTED_IDENTIFIER_PART+
    ;

fragment SRC_UNQUOTED_IDENTIFIER_PART
    : ~[=`|,[\]/ \t\r\n]+
    | '/' ~[*/] // allow single / but not followed by another / or * which would start a comment
    ;

SRC_QUOTED_IDENTIFIER
    : QUOTED_IDENTIFIER
    ;

SRC_LINE_COMMENT
    : LINE_COMMENT -> channel(HIDDEN)
    ;

SRC_MULTILINE_COMMENT
    : MULTILINE_COMMENT -> channel(HIDDEN)
    ;

SRC_WS
    : WS -> channel(HIDDEN)
    ;

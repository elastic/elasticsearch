lexer grammar EsqlBaseLexer;

EVAL : 'eval' -> pushMode(EXPRESSION);
EXPLAIN : 'explain' -> pushMode(EXPRESSION);
FROM : 'from' -> pushMode(SOURCE_IDENTIFIERS);
ROW : 'row' -> pushMode(EXPRESSION);
STATS : 'stats' -> pushMode(EXPRESSION);
INLINESTATS : 'inlinestats' -> pushMode(EXPRESSION);
WHERE : 'where' -> pushMode(EXPRESSION);
SORT : 'sort' -> pushMode(EXPRESSION);
LIMIT : 'limit' -> pushMode(EXPRESSION);
PROJECT : 'project' -> pushMode(SOURCE_IDENTIFIERS);
SHOW : 'show' -> pushMode(EXPRESSION);
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
OPENING_BRACKET : '[' -> pushMode(DEFAULT_MODE);
CLOSING_BRACKET : ']' -> popMode, popMode; // pop twice, once to clear mode of current cmd and once to exit DEFAULT_MODE
NOT : 'not';
NULL : 'null';
NULLS : 'nulls';
OR : 'or';
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

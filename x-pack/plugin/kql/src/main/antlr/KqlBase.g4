/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

grammar KqlBase;

topLevelQuery
    : query EOF
    ;

query
    : query (OR query)+                                  #logicalOr
    | query (AND query)+                                 #logicalAnd
    | NOT query                                          #logicalNot
    | nestedQuery                                        #queryDefault
    | expression                                         #queryDefault
    | LEFT_PARENTHESIS query RIGHT_PARENTHESIS           #queryDefault
    ;

expression
    : fieldMTermQuery
    | fieldRangeQuery
    ;

nestedQuery:
    fieldName COLON LEFT_CURLY_BRACKET query RIGHT_CURLY_BRACKET;

fieldRangeQuery
    : fieldName operator=OP_COMPARE term
    ;

fieldMTermQuery
    : ( fieldName ( COLON ))? ( term+ | groupingExpr)
    ;

term
    : QUOTED
    | NUMBER
    | LITERAL
    ;

groupingExpr
    : LEFT_PARENTHESIS term+ RIGHT_PARENTHESIS?
    ;

fieldName
    : LITERAL
    ;

AND: [Aa][Nn][Dd];
OR: 'OR' | '||';
NOT: 'NOT' | '!';
LEFT_PARENTHESIS: '(';
RIGHT_PARENTHESIS: ')';

LEFT_CURLY_BRACKET: '{';
RIGHT_CURLY_BRACKET: '}';

COLON: ':';

fragment OP_LESS: '<';
fragment OP_LESS_EQ: '<=';
fragment OP_MORE: '>';
fragment OP_MORE_EQ: '>=';
OP_COMPARE: OP_LESS | OP_MORE | OP_LESS_EQ | OP_MORE_EQ;

QUOTED: '"' QUOTED_CHAR* '"';
NUMBER: NUM_CHAR+ ( '.' NUM_CHAR+)?;
LITERAL: TERM_CHAR+;

fragment QUOTED_CHAR: ~["\\] | ESCAPED_CHAR;
fragment ESCAPED_CHAR: '\\' .;
fragment NUM_CHAR: [0-9];
fragment TERM_CHAR: ~[ \t\n\r\u3000():<>"{}\\/] | ESCAPED_CHAR;

DEFAULT_SKIP: WHITESPACE -> skip;

fragment WHITESPACE: [ \t\n\r\u3000];

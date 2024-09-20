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
    : query OR query                                 #logicalOr
    | query AND query                                #logicalAnd
    | NOT subQuery=query                             #logicalNot
    | nestedQuery                                    #queryDefault
    | expression                                     #queryDefault
    | LEFT_PARENTHESIS query RIGHT_PARENTHESIS       #parenthesizedQuery
    ;

expression
    : fieldTermQuery
    | fieldRangeQuery
    ;

nestedQuery
    : fieldName COLON LEFT_CURLY_BRACKET query RIGHT_CURLY_BRACKET
    ;

fieldRangeQuery
    : fieldName operator=OP_COMPARE termValue
    ;

fieldTermQuery
    : (fieldName (COLON))? ( termValue+ | groupingExpr )
    ;

termValue
    : QUOTED_STRING
    | UNQUOTED_LITERAL
    ;

groupingExpr
    : LEFT_PARENTHESIS termValue+ RIGHT_PARENTHESIS
    ;

fieldName
    : QUOTED_STRING
    | UNQUOTED_LITERAL
    ;

DEFAULT_SKIP: WHITESPACE -> skip;

AND: [Aa][Nn][Dd];
OR: [Oo][Rr];
NOT: [Nn][Oo][Tt];

COLON: ':';
OP_COMPARE: OP_LESS | OP_MORE | OP_LESS_EQ | OP_MORE_EQ;

LEFT_PARENTHESIS: '(';
RIGHT_PARENTHESIS: ')';
LEFT_CURLY_BRACKET: '{';
RIGHT_CURLY_BRACKET: '}';

UNQUOTED_LITERAL: UNQUOTED_CHAR+;
QUOTED_STRING: '"' QUOTED_CHAR* '"';

LITERAL: QUOTED_STRING | UNQUOTED_LITERAL;

fragment WILDCARD: '*';
fragment OP_LESS: '<';
fragment OP_LESS_EQ: '<=';
fragment OP_MORE: '>';
fragment OP_MORE_EQ: '>=';

fragment UNQUOTED_CHAR
    : ESCAPED_WHITESPACE
    | ESCAPED_SPECIAL_CHAR
    | ESCAPE_UNICODE_SEQUENCE
    // TODO add escaped keyword
    | WILDCARD
    | NON_SPECIAL_CHAR
    ;

fragment QUOTED_CHAR
    : ESCAPED_WHITESPACE
    | ESCAPE_UNICODE_SEQUENCE
    | ESCAPED_QUOTE
    | ~["]
    ;


fragment WHITESPACE: [ \t\n\r\u3000];
fragment ESCAPED_WHITESPACE: '\\r' | '\\t' | '\\n';

fragment NON_SPECIAL_CHAR: ~[ \\():<>"*{}];
fragment ESCAPED_SPECIAL_CHAR: '\\'[\\():<>"*{}];

fragment ESCAPE_UNICODE_SEQUENCE: '\\';
fragment ESCAPED_QUOTE: '\\"';

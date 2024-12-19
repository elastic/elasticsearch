/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

grammar KqlBase;


@header {
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
}

options {
  caseInsensitive=true;
}

topLevelQuery
    : query? EOF
    ;

query
    : <assoc=right> query operator=(AND|OR) query  #booleanQuery
    | simpleQuery                                  #defaultQuery
    ;

simpleQuery
    : notQuery
    | nestedQuery
    | parenthesizedQuery
    | matchAllQuery
    | existsQuery
    | rangeQuery
    | fieldQuery
    | fieldLessQuery
    ;

notQuery:
   NOT subQuery=simpleQuery
   ;

nestedQuery
    : fieldName COLON LEFT_CURLY_BRACKET nestedSubQuery RIGHT_CURLY_BRACKET
    ;

nestedSubQuery
    : <assoc=right> nestedSubQuery operator=(AND|OR) nestedSubQuery #booleanNestedQuery
    | nestedSimpleSubQuery                                          #defaultNestedQuery
    ;

nestedSimpleSubQuery
    : notQuery
    | nestedQuery
    | matchAllQuery
    | nestedParenthesizedQuery
    | existsQuery
    | rangeQuery
    | fieldQuery;

nestedParenthesizedQuery
    : LEFT_PARENTHESIS nestedSubQuery RIGHT_PARENTHESIS;

matchAllQuery
    : (WILDCARD COLON)? WILDCARD
    ;

parenthesizedQuery
    : LEFT_PARENTHESIS query RIGHT_PARENTHESIS
    ;

rangeQuery
    : fieldName operator=(OP_LESS|OP_LESS_EQ|OP_MORE|OP_MORE_EQ) rangeQueryValue
    ;

rangeQueryValue
    : (UNQUOTED_LITERAL|WILDCARD)+
    | QUOTED_STRING
   ;

existsQuery
    :fieldName COLON WILDCARD
    ;

fieldQuery
    : fieldName COLON fieldQueryValue
    | fieldName COLON LEFT_PARENTHESIS fieldQueryValue RIGHT_PARENTHESIS
    ;

fieldLessQuery
    : fieldQueryValue
    | LEFT_PARENTHESIS fieldQueryValue RIGHT_PARENTHESIS
    ;

fieldQueryValue
    : (AND|OR|NOT)? (UNQUOTED_LITERAL|WILDCARD)+ (NOT|AND|OR)?
    | (AND|OR) (AND|OR|NOT)?
    | NOT (AND|OR)?
    | QUOTED_STRING
    ;

fieldName
    : value=UNQUOTED_LITERAL
    | value=QUOTED_STRING
    | value=WILDCARD
    ;

DEFAULT_SKIP: WHITESPACE -> skip;

AND: 'and';
OR: 'or';
NOT: 'not';

COLON: ':';
OP_LESS: '<';
OP_LESS_EQ: '<=';
OP_MORE: '>';
OP_MORE_EQ: '>=';

LEFT_PARENTHESIS: '(';
RIGHT_PARENTHESIS: ')';
LEFT_CURLY_BRACKET: '{';
RIGHT_CURLY_BRACKET: '}';

UNQUOTED_LITERAL: UNQUOTED_LITERAL_CHAR+;

QUOTED_STRING: '"'QUOTED_CHAR*'"';

WILDCARD: WILDCARD_CHAR;

fragment WILDCARD_CHAR: '*';

fragment UNQUOTED_LITERAL_CHAR
    : WILDCARD_CHAR* UNQUOTED_LITERAL_BASE_CHAR WILDCARD_CHAR*
    | WILDCARD_CHAR WILDCARD_CHAR+
    ;

fragment UNQUOTED_LITERAL_BASE_CHAR
    : ESCAPED_WHITESPACE
    | ESCAPED_SPECIAL_CHAR
    | ESCAPE_UNICODE_SEQUENCE
    | '\\' (AND | OR | NOT)
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
fragment NON_SPECIAL_CHAR: ~[ \n\r\t\u3000\\():<>"*{}];
fragment ESCAPED_SPECIAL_CHAR: '\\'[ \\():<>"*{}];

fragment ESCAPED_QUOTE: '\\"';

fragment ESCAPE_UNICODE_SEQUENCE: '\\' UNICODE_SEQUENCE;
fragment UNICODE_SEQUENCE: 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT;
fragment HEX_DIGIT: [0-9a-f];

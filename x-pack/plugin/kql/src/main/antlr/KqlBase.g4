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
    : query (AND | OR) query          #booleanQuery
    | NOT subQuery=simpleQuery        #notQuery
    | simpleQuery                     #defaultQuery
    ;

simpleQuery
    : nestedQuery
    | expression
    | parenthesizedQuery
    ;

expression
    : fieldTermQuery
    | fieldRangeQuery
    ;

nestedQuery
    : fieldName COLON LEFT_CURLY_BRACKET query RIGHT_CURLY_BRACKET
    ;

parenthesizedQuery:
   LEFT_PARENTHESIS query RIGHT_PARENTHESIS;

fieldRangeQuery
    : fieldName operator=OP_COMPARE rangeQueryValue
    ;

fieldTermQuery
    : (fieldName COLON)? termQueryValue
    ;

fieldName
    : wildcardExpression
    | unquotedLiteralExpression
    | quotedStringExpression
    ;

rangeQueryValue
    : unquotedLiteralExpression
    | quotedStringExpression
    ;

termQueryValue
    : wildcardExpression
    | quotedStringExpression
    | termValue=unquotedLiteralExpression
    | groupingTermExpression;

groupingTermExpression
    : LEFT_PARENTHESIS unquotedLiteralExpression RIGHT_PARENTHESIS
    ;

unquotedLiteralExpression
    : UNQUOTED_LITERAL+
    ;

quotedStringExpression
    : QUOTED_STRING
    ;

wildcardExpression
    : WILDCARD
;


DEFAULT_SKIP: WHITESPACE -> skip;

AND: 'and';
OR: 'or';
NOT: 'not';

COLON: ':';
OP_COMPARE: OP_LESS | OP_MORE | OP_LESS_EQ | OP_MORE_EQ;

LEFT_PARENTHESIS: '(';
RIGHT_PARENTHESIS: ')';
LEFT_CURLY_BRACKET: '{';
RIGHT_CURLY_BRACKET: '}';

UNQUOTED_LITERAL: WILDCARD* UNQUOTED_LITERAL_CHAR+ WILDCARD*;

QUOTED_STRING: '"'QUOTED_CHAR*'"';

WILDCARD: WILDCARD_CHAR+;

fragment WILDCARD_CHAR: '*';
fragment OP_LESS: '<';
fragment OP_LESS_EQ: '<=';
fragment OP_MORE: '>';
fragment OP_MORE_EQ: '>=';

fragment UNQUOTED_LITERAL_CHAR
    : ESCAPED_WHITESPACE
    | ESCAPED_SPECIAL_CHAR
    | ESCAPE_UNICODE_SEQUENCE
    | '\\' (AND | OR | NOT)
    | WILDCARD_CHAR UNQUOTED_LITERAL_CHAR
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
fragment ESCAPED_SPECIAL_CHAR: '\\'[ \\():<>"*{}];

fragment ESCAPED_QUOTE: '\\"';

fragment ESCAPE_UNICODE_SEQUENCE: '\\' UNICODE_SEQUENCE;
fragment UNICODE_SEQUENCE: 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT;
fragment HEX_DIGIT: [0-9a-f];

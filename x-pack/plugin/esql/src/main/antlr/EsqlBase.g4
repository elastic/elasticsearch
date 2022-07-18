/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

grammar EsqlBase;

statement
    : query
    ;

query
    : sourceCmd
    ;

sourceCmd
    : rowCmd
    ;

rowCmd
    : ROW fields
    ;

fields
    : field (COMMA field)*
    ;

field
    : expression
    | identifier EQUALS expression
    ;

expression : INTEGER_LITERAL;

identifier : IDENTIFIER;

fragment DIGIT : [0-9];
fragment LETTER : [A-Za-z];

INTEGER_LITERAL : DIGIT+;

ROW : 'row';

COMMA : ',';
EQUALS : '=';

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

parser grammar PainlessParser;

options { tokenVocab=PainlessLexer; }

sourceBlock
    : shortStatement* EOF
    ;

shortStatementBlock
    : statementBlock
    | shortStatement
    ;

longStatementBlock
    : statementBlock
    | longStatement
    ;

statementBlock
    : LBRACK shortStatement* RBRACK
    ;

emptyStatement
    : SEMICOLON
    ;

shortStatement
    : noTrailingStatement
    | shortIfStatement
    | longIfShortElseStatement
    | shortWhileStatement
    | shortForStatement
    ;

longStatement
    : noTrailingStatement
    | longIfStatement
    | longWhileStatement
    | longForStatement
    ;

noTrailingStatement
    : declarationStatement delimiter
    | doStatement delimiter
    | continueStatement delimiter
    | breakStatement delimiter
    | returnStatement delimiter
    | tryStatement
    | throwStatement
    | expressionStatement delimiter
    ;

shortIfStatement
    : IF LP expression RP shortStatementBlock
    ;

longIfShortElseStatement
    : IF LP expression RP longStatementBlock ELSE shortStatementBlock
    ;

longIfStatement
    : IF LP expression RP longStatementBlock ELSE longStatementBlock
    ;

shortWhileStatement
    : WHILE LP expression RP ( shortStatementBlock | emptyStatement )
    ;

longWhileStatement
    : WHILE LP expression RP ( longStatementBlock | emptyStatement )
    ;

shortForStatement
    : FOR LP forInitializer? SEMICOLON expression? SEMICOLON forAfterthought? RP ( shortStatementBlock | emptyStatement )
    ;

longForStatement
    : FOR LP forInitializer? SEMICOLON expression? SEMICOLON forAfterthought? RP ( longStatementBlock | emptyStatement )
    ;

doStatement
    : DO statementBlock WHILE LP expression RP
    ;

declarationStatement
    : declarationType declarationVariable ( COMMA declarationVariable )*
    ;

continueStatement
    : CONTINUE
    ;

breakStatement
    : BREAK
    ;

returnStatement
    : RETURN expression
    ;

tryStatement
    : TRY statementBlock catchBlock+
    ;

throwStatement
    : THROW expression
    ;

expressionStatement
    : expression
    ;

forInitializer
    : declarationStatement
    | expression
    ;

forAfterthought
    : expression
    ;

declarationType
    : type (LBRACE RBRACE)*
    ;

type
    : TYPE (DOT DOTTYPE)*
    ;

declarationVariable
    : ID ( ASSIGN expression )?
    ;

catchBlock
    : CATCH LP ( type ID ) RP ( statementBlock )
    ;

delimiter
    : SEMICOLON
    | EOF
    ;

expression returns [boolean s = true]
    :               u = unary                                             { $s = $u.s; }           # single
    |               expression ( MUL | DIV | REM ) expression             { $s = false; }          # binary
    |               expression ( ADD | SUB ) expression                   { $s = false; }          # binary
    |               expression ( LSH | RSH | USH ) expression             { $s = false; }          # binary
    |               expression ( LT | LTE | GT | GTE ) expression         { $s = false; }          # comp
    |               expression ( EQ | EQR | NE | NER ) expression         { $s = false; }          # comp
    |               expression BWAND expression                           { $s = false; }          # binary
    |               expression XOR expression                             { $s = false; }          # binary
    |               expression BWOR expression                            { $s = false; }          # binary
    |               expression BOOLAND expression                         { $s = false; }          # bool
    |               expression BOOLOR expression                          { $s = false; }          # bool
    | <assoc=right> expression COND e0 = expression COLON e1 = expression { $s = $e0.s && $e1.s; } # conditional
    |               chain ( ASSIGN | AADD | ASUB | AMUL |
                            ADIV   | AREM | AAND | AXOR |
                            AOR    | ALSH | ARSH | AUSH ) expression                               # assignment
    ;

unary returns [boolean s = true]
    : ( INCR | DECR ) chain                                 # pre
    | chain (INCR | DECR )                                  # post
    | chain                                                 # read
    | ( OCTAL | HEX | INTEGER | DECIMAL )   { $s = false; } # numeric
    | TRUE                                  { $s = false; } # true
    | FALSE                                 { $s = false; } # false
    | NULL                                  { $s = false; } # null
    | ( BOOLNOT | BWNOT | ADD | SUB ) unary                 # operator
    | LP declarationType RP unary                           # cast
    ;

chain
    : p = primary secondary[$p.s]*                              # dynamicprimary
    | declarationType dotsecondary secondary[true]*             # staticprimary
    | NEW type bracesecondary+ (dotsecondary secondary[true]*)? # newarray
    ;

primary returns [boolean s = true]
    : LP e = expression RP { $s = $e.s; } # precedence
    | STRING                              # string
    | ID                                  # variable
    | NEW type arguments                  # newobject
    ;

secondary [boolean s]
    : { $s }? dotsecondary
    | { $s }? bracesecondary
    ;

dotsecondary
    : DOT DOTID arguments        # callinvoke
    | DOT ( DOTID | DOTINTEGER ) # fieldaccess
    ;

bracesecondary
    : LBRACE expression RBRACE # braceaccess
    ;

arguments
    : ( LP ( expression ( COMMA expression )* )? RP )
    ;

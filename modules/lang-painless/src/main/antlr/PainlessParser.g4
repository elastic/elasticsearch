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

source
    : statement* EOF
    ;

statement
    : IF LP expression RP trailer ( ELSE trailer | { _input.LA(1) != ELSE }? )                 # if
    | WHILE LP expression RP ( trailer | empty )                                               # while
    | DO block WHILE LP expression RP delimiter                                                # do
    | FOR LP initializer? SEMICOLON expression? SEMICOLON afterthought? RP ( trailer | empty ) # for
    | declaration delimiter                                                                    # decl
    | CONTINUE delimiter                                                                       # continue
    | BREAK delimiter                                                                          # break
    | RETURN expression delimiter                                                              # return
    | TRY block trap+                                                                          # try
    | THROW expression delimiter                                                               # throw
    | expression delimiter                                                                     # expr
    ;

trailer
    : block
    | statement
    ;

block
    : LBRACK statement* RBRACK
    ;

empty
    : SEMICOLON
    ;

initializer
    : declaration
    | expression
    ;

afterthought
    : expression
    ;

declaration
    : decltype declvar (COMMA declvar)*
    ;

decltype
    : TYPE (LBRACE RBRACE)*
    ;

declvar
    : ID ( ASSIGN expression )?
    ;

trap
    : CATCH LP TYPE ID RP block
    ;

delimiter
    : SEMICOLON
    | EOF
    ;

expression returns [boolean s = true]
    :               u = unary[false]                                       { $s = $u.s; }           # single
    |               expression ( MUL | DIV | REM ) expression              { $s = false; }          # binary
    |               expression ( ADD | SUB ) expression                    { $s = false; }          # binary
    |               expression ( LSH | RSH | USH ) expression              { $s = false; }          # binary
    |               expression ( LT | LTE | GT | GTE ) expression          { $s = false; }          # comp
    |               expression ( EQ | EQR | NE | NER ) expression          { $s = false; }          # comp
    |               expression BWAND expression                            { $s = false; }          # binary
    |               expression XOR expression                              { $s = false; }          # binary
    |               expression BWOR expression                             { $s = false; }          # binary
    |               expression BOOLAND expression                          { $s = false; }          # bool
    |               expression BOOLOR expression                           { $s = false; }          # bool
    | <assoc=right> expression COND e0 = expression COLON e1 = expression  { $s = $e0.s && $e1.s; } # conditional
    // TODO: Should we allow crazy syntax like (x = 5).call()?
    //       Other crazy syntaxes work, but this one requires
    //       a complete restructure of the rules as EChain isn't
    //       designed to handle more postfixes after an assignment.
    | <assoc=right> chain[true] ( ASSIGN | AADD | ASUB | AMUL |
                                  ADIV   | AREM | AAND | AXOR |
                                  AOR    | ALSH | ARSH | AUSH ) expression { $s = false; }         # assignment
    ;

unary[boolean c] returns [boolean s = true]
    : { !$c }? ( INCR | DECR ) chain[true]                                  # pre
    | { !$c }? chain[true] (INCR | DECR )                                   # post
    | { !$c }? chain[false]                                                 # read
    | { !$c }? ( OCTAL | HEX | INTEGER | DECIMAL )          { $s = false; } # numeric
    | { !$c }? TRUE                                         { $s = false; } # true
    | { !$c }? FALSE                                        { $s = false; } # false
    | { !$c }? NULL                                         { $s = false; } # null
    | { !$c }? ( BOOLNOT | BWNOT | ADD | SUB ) unary[false]                 # operator
    |          LP decltype RP unary[$c]                                     # cast
    ;

chain[boolean c]
    : p = primary[$c] secondary[$p.s]*                             # dynamic
    | decltype dot secondary[true]*                                # static
    | NEW TYPE (LBRACE expression RBRACE)+ (dot secondary[true]*)? # newarray
    ;

primary[boolean c] returns [boolean s = true]
    : { !$c }? LP e = expression RP { $s = $e.s; } # exprprec
    | { $c }?  LP unary[true] RP                   # chainprec
    |          STRING                              # string
    |          ID                                  # variable
    |          NEW TYPE arguments                  # newobject
    ;

secondary[boolean s]
    : { $s }? dot
    | { $s }? brace
    ;

dot
    : DOT DOTID arguments        # callinvoke
    | DOT ( DOTID | DOTINTEGER ) # fieldaccess
    ;

brace
    : LBRACE expression RBRACE # braceaccess
    ;

arguments
    : ( LP ( expression ( COMMA expression )* )? RP )
    ;

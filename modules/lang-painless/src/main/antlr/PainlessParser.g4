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
    : function* statement* EOF
    ;

function
    : decltype ID parameters block
    ;

parameters
    : LP ( decltype ID ( COMMA decltype ID )* )? RP
    ;

// Note we use a predicate on the if/else case here to prevent the
// "dangling-else" ambiguity by forcing the 'else' token to be consumed
// as soon as one is found.  See (https://en.wikipedia.org/wiki/Dangling_else).
statement
    : IF LP expression RP trailer ( ELSE trailer | { _input.LA(1) != ELSE }? )                 # if
    | WHILE LP expression RP ( trailer | empty )                                               # while
    | DO block WHILE LP expression RP delimiter                                                # do
    | FOR LP initializer? SEMICOLON expression? SEMICOLON afterthought? RP ( trailer | empty ) # for
    | FOR LP decltype ID COLON expression RP trailer                                           # each
    | FOR LP ID IN expression RP trailer                                                       # ineach
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

expression
    :               unary                                                 # single
    |               expression ( MUL | DIV | REM ) expression             # binary
    |               expression ( ADD | SUB ) expression                   # binary
    |               expression ( FIND | MATCH ) expression                # binary
    |               expression ( LSH | RSH | USH ) expression             # binary
    |               expression ( LT | LTE | GT | GTE ) expression         # comp
    |               expression INSTANCEOF decltype                        # instanceof
    |               expression ( EQ | EQR | NE | NER ) expression         # comp
    |               expression BWAND expression                           # binary
    |               expression XOR expression                             # binary
    |               expression BWOR expression                            # binary
    |               expression BOOLAND expression                         # bool
    |               expression BOOLOR expression                          # bool
    | <assoc=right> expression COND expression COLON expression           # conditional
    | <assoc=right> expression ELVIS expression                           # elvis
    | <assoc=right> expression ( ASSIGN | AADD | ASUB | AMUL |
                                 ADIV   | AREM | AAND | AXOR |
                                 AOR    | ALSH | ARSH | AUSH ) expression # assignment
    ;

unary
    :  ( INCR | DECR ) chain                 # pre
    |  chain (INCR | DECR )                  # post
    |  chain                                 # read
    |  ( BOOLNOT | BWNOT | ADD | SUB ) unary # operator
    |  LP decltype RP unary                  # cast
    ;

chain
    : primary postfix*          # dynamic
    | decltype postdot postfix* # static
    | arrayinitializer          # newarray
    ;

primary
    : LP expression RP                    # precedence
    | ( OCTAL | HEX | INTEGER | DECIMAL ) # numeric
    | TRUE                                # true
    | FALSE                               # false
    | NULL                                # null
    | STRING                              # string
    | REGEX                               # regex
    | listinitializer                     # listinit
    | mapinitializer                      # mapinit
    | ID                                  # variable
    | ID arguments                        # calllocal
    | NEW TYPE arguments                  # newobject
    ;

postfix
    : callinvoke
    | fieldaccess
    | braceaccess
    ;

postdot
    : callinvoke
    | fieldaccess
    ;

callinvoke
    : ( DOT | NSDOT ) DOTID arguments
    ;

fieldaccess
    : ( DOT | NSDOT ) ( DOTID | DOTINTEGER )
    ;

braceaccess
    : LBRACE expression RBRACE
    ;

arrayinitializer
    : NEW TYPE ( LBRACE expression RBRACE )+ ( postdot postfix* )?                                   # newstandardarray
    | NEW TYPE LBRACE RBRACE LBRACK ( expression ( COMMA expression )* )? SEMICOLON? RBRACK postfix* # newinitializedarray
    ;

listinitializer
    : LBRACE expression ( COMMA expression)* RBRACE
    | LBRACE RBRACE
    ;

mapinitializer
    : LBRACE maptoken ( COMMA maptoken )* RBRACE
    | LBRACE COLON RBRACE
    ;

maptoken
    : expression COLON expression
    ;

arguments
    : ( LP ( argument ( COMMA argument )* )? RP )
    ;

argument
    : expression
    | lambda
    | funcref
    ;

lambda
    : ( lamtype | LP ( lamtype ( COMMA lamtype )* )? RP ) ARROW ( block | expression )
    ;

lamtype
    : decltype? ID
    ;

funcref
    : TYPE REF ID      # classfuncref       // reference to a static or instance method,
                                            // e.g. ArrayList::size or Integer::compare
    | decltype REF NEW # constructorfuncref // reference to a constructor, e.g. ArrayList::new
    | ID REF ID        # capturingfuncref   // reference to an instance method, e.g. object::toString
                                            // currently limited to capture of a simple variable (id).
    | THIS REF ID      # localfuncref       // reference to a local function, e.g. this::myfunc
    ;

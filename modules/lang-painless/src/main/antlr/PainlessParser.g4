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

statement
    : rstatement
    | dstatement ( SEMICOLON | EOF )
    ;

// Note we use a predicate on the if/else case here to prevent the
// "dangling-else" ambiguity by forcing the 'else' token to be consumed
// as soon as one is found.  See (https://en.wikipedia.org/wiki/Dangling_else).
rstatement
    : IF LP expression RP trailer ( ELSE trailer | { _input.LA(1) != ELSE }? )                 # if
    | WHILE LP expression RP ( trailer | empty )                                               # while
    | FOR LP initializer? SEMICOLON expression? SEMICOLON afterthought? RP ( trailer | empty ) # for
    | FOR LP decltype ID COLON expression RP trailer                                           # each
    | FOR LP ID IN expression RP trailer                                                       # ineach
    | TRY block trap+                                                                          # try
    ;

dstatement
    : DO block WHILE LP expression RP # do
    | declaration                     # decl
    | CONTINUE                        # continue
    | BREAK                           # break
    | RETURN expression?              # return
    | THROW expression                # throw
    | expression                      # expr
    ;

trailer
    : block
    | statement
    ;

block
    : LBRACK statement* dstatement? RBRACK
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

noncondexpression
    :               unary                                                               # single
    |               noncondexpression ( MUL | DIV | REM ) noncondexpression             # binary
    |               noncondexpression ( ADD | SUB ) noncondexpression                   # binary
    |               noncondexpression ( FIND | MATCH ) noncondexpression                # binary
    |               noncondexpression ( LSH | RSH | USH ) noncondexpression             # binary
    |               noncondexpression ( LT | LTE | GT | GTE ) noncondexpression         # comp
    |               noncondexpression INSTANCEOF decltype                               # instanceof
    |               noncondexpression ( EQ | EQR | NE | NER ) noncondexpression         # comp
    |               noncondexpression BWAND noncondexpression                           # binary
    |               noncondexpression XOR noncondexpression                             # binary
    |               noncondexpression BWOR noncondexpression                            # binary
    |               noncondexpression BOOLAND noncondexpression                         # bool
    |               noncondexpression BOOLOR noncondexpression                          # bool
    | <assoc=right> noncondexpression ELVIS noncondexpression                           # elvis
    ;

expression
    :               noncondexpression                                            # nonconditional
    | <assoc=right> noncondexpression COND expression COLON expression           # conditional
    | <assoc=right> noncondexpression ( ASSIGN | AADD | ASUB | AMUL |
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
    : NEW TYPE ( LBRACE expression RBRACE )+ ( postdot postfix* )?                        # newstandardarray
    | NEW TYPE LBRACE RBRACE LBRACK ( expression ( COMMA expression )* )? RBRACK postfix* # newinitializedarray
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
    : TYPE REF ID      # classfuncref
    | decltype REF NEW # constructorfuncref
    | ID REF ID        # capturingfuncref
    | THIS REF ID      # localfuncref
    ;

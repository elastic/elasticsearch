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
    : shortStatement+ EOF
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
    : assignment
    ;

forInitializer
    : declarationStatement
    | expression
    ;

forAfterthought
    : expression
    ;

declarationType
    : ID (LBRACE RBRACE)*
    ;

declarationVariable
    : ID ( ASSIGN expression )?
    ;

catchBlock
    : CATCH LP ( ID ID ) RP ( statementBlock )
    ;

delimiter
    : SEMICOLON
    | EOF
    ;

assignment
    : lhs ( ASSIGN | AADD | ASUB | AMUL |
            ADIV   | AREM | AAND | AXOR |
            AOR    | ALSH | ARSH | AUSH ) ( conditional | assignment )
    ;

lhs
    : ID
    | field
    | array
    ;

conditional
    : booleanOr
    | booleanOr COND booleanOr COLON booleanOr
    ;

booleanOr
    : booleanAnd
    | booleanAnd OR booleanOr
    ;

booleanAnd
    : bitOr
    | bitOr AND booleanAnd
    ;

bitOr
    : bitXor
    | bitXor BWOR bitOr
    ;

bitXor
    : bitAnd
    | bitAnd BWXOR bitXor
    ;

bitAnd
    : equality
    | equality BWAND bitAnd
    ;

equality
    : comparison
    | comparison ( EQ | EQR | NE | NER ) equality
    ;

comparison
    : shift
    | shift ( LT | LTE | GT | GTE ) comparison
    ;

shift
    : additive
    | additive ( LSH | RSH | USH ) shift
    ;

additive
    : multiplicative
    | multiplicative ( ADD | SUB ) additive
    ;

multiplicative
    : unary
    | unary ( MUL | DIV | REM ) multiplicative
    ;

unary
    :
    ;

preIncrement

preDecerement

expression
    :               LP expression RP                                    # precedence
    |               ( OCTAL | HEX | INTEGER | DECIMAL )                 # numeric
    |               TRUE                                                # true
    |               FALSE                                               # false
    |               NULL                                                # null
    | <assoc=right> chain ( INCR | DECR )                               # postinc
    | <assoc=right> ( INCR | DECR ) chain                               # preinc
    |               chain                                               # read
    | <assoc=right> ( BOOLNOT | BWNOT | ADD | SUB ) expression          # unary
    | <assoc=right> LP declarationType RP expression                    # cast
    |               expression ( MUL | DIV | REM ) expression           # binary
    |               expression ( ADD | SUB ) expression                 # binary
    |               expression ( LSH | RSH | USH ) expression           # binary
    |               expression ( LT | LTE | GT | GTE ) expression       # comp
    |               expression ( EQ | EQR | NE | NER ) expression       # comp
    |               expression BWAND expression                         # binary
    |               expression XOR expression                           # binary
    |               expression BWOR expression                          # binary
    |               expression BOOLAND expression                       # bool
    |               expression BOOLOR expression                        # bool
    | <assoc=right> expression COND expression COLON expression         # conditional
    ;

chain
    : linkprec
    | linkcast
    | linkvar
    | linknew
    | linkstring
    ;

linkprec:   LP ( linkprec | linkcast | linkvar | linknew | linkstring ) RP ( linkdot | linkbrace )?;
linkcast:   LP declarationType RP ( linkprec | linkcast | linkvar | linknew | linkstring );
linkbrace:  LBRACE expression RBRACE ( linkdot | linkbrace )?;
linkdot:    DOT ( linkcall | linkfield );
linkcall:   EXTID arguments ( linkdot | linkbrace )?;
linkvar:    ID ( linkdot | linkbrace )?;
linkfield:  ( EXTID | EXTINTEGER ) ( linkdot | linkbrace )?;
linknew:    NEW ID ( ( arguments linkdot? ) | ( ( LBRACE expression RBRACE )+ linkdot? ) );
linkstring: STRING (linkdot | linkbrace )?;

arguments
    : ( LP ( expression ( COMMA expression )* )? RP )
    ;


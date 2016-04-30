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

package org.elasticsearch.painless.tree.node;

public enum Type {
    SOURCE ( "source" ),
    IF ( "if" ),
    WHILE ( "while" ),
    DO ( "do" ),
    FOR ( "for" ),
    CONTINUE ( "continue" ),
    BREAK ( "break" ),
    RETURN ( "return" ),
    TRY ( "try" ),
    TRAP ( "trap" ),
    THROW ( "throw" ),
    EXPRESSION ( "expression" ),
    BLOCK ( "block" ),
    DECLARATION ( "declaration" ),
    DECLVAR ( "declvar" ),
    NUMERIC ( "numeric" ),
    CHAR ( "char" ),
    TRUE ( "true" ),
    FALSE ( "false" ),
    NULL ( "null" ),
    CAST ( "cast" ),
    UNARY ( "unary" ),
    BINARY ( "binary" ),
    CONDITIONAL ( "conditional" ),
    EXTERNAL ( "external" ),
    POST ( "post" ),
    PRE ( "pre" ),
    ASSIGNMENT ( "assignment" ),
    COMPOUND ( "compound" ),
    BRACE ( "brace" ),
    CALL ( "call" ),
    VAR ( "var" ),
    FIELD ( "field" ),
    NEWOBJ ( "newobj" ),
    NEWARRAY ( "newarray" ),
    STRING ( "string" ),

    ACAST ( "acast" ),
    ATRANSFORM ( "atranform" ),
    ACONSTANT ( "aconstant" ),
    ASHORTCUT ( "ashortcut" ),
    AMAPSHORTCUT ( "amapshortcut" ),
    ALISTSHORTCUT ( "alistshorcut" ),
    ADEFBRACE ( "adefbrace" ),
    ADEFCALL ( "adefcall" ),
    ADEFFIELD ( "adeffield" ),
    AALENGTH ( "aalength" ),

    TBRANCH ( "tbranch" ),
    TMARK ( "tmark" ),
    TGOTO ( "tgoto" ),
    TLOOPCOUNT ( "tloopcount" ),
    TTRAP ( "ttrap" ),
    TVARLOAD ( "tvarload" ),
    TVARSTORE ( "tvarstore" ),
    TDUP ( "tdup" ),
    TPOP ( "tpop" );

    public final String symbol;

    Type(final String symbol) {
        this.symbol = symbol;
    }
}

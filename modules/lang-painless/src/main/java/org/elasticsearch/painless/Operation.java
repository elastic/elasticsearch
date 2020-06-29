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

package org.elasticsearch.painless;

/**
 * Provides a way to represent operations independently of ASM, to keep ASM
 * contained to only the writing phase of compilation.  Note there are also
 * a few extra operations not in ASM that are used internally by the
 * Painless tree.
 */
public enum Operation {

    MUL     ( "*"   , "multiplication"         ),
    DIV     ( "/"   , "division"               ),
    REM     ( "%"   , "remainder"              ),
    ADD     ( "+"   , "addition"               ),
    SUB     ( "-"   , "subtraction"            ),
    FIND    ( "=~"  , "find"                   ),
    MATCH   ( "==~" , "match"                  ),
    LSH     ( "<<"  , "left shift"             ),
    RSH     ( ">>"  , "right shift"            ),
    USH     ( ">>>" , "unsigned shift"         ),
    BWNOT   ( "~"   , "bitwise not"            ),
    BWAND   ( "&"   , "bitwise and"            ),
    XOR     ( "^"   , "bitwise xor"            ),
    BWOR    ( "|"   , "boolean or"             ),
    NOT     ( "!"   , "boolean not"            ),
    AND     ( "&&"  , "boolean and"            ),
    OR      ( "||"  , "boolean or"             ),
    LT      ( "<"   , "less than"              ),
    LTE     ( "<="  , "less than or equals"    ),
    GT      ( ">"   , "greater than"           ),
    GTE     ( ">="  , "greater than or equals" ),
    EQ      ( "=="  , "equals"                 ),
    EQR     ( "===" , "reference equals"       ),
    NE      ( "!="  , "not equals"             ),
    NER     ( "!==" , "reference not equals"   );

    public final String symbol;
    public final String name;

    Operation(final String symbol, final String name) {
        this.symbol = symbol;
        this.name = name;
    }
}

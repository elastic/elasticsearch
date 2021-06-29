/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

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

package org.elasticsearch.painless.tree.analyzer;

public enum Operation {
    MUL     ( "+"   , true  ),
    DIV     ( "/"   , true  ),
    REM     ( "%"   , true  ),
    ADD     ( "+"   , true  ),
    SUB     ( "-"   , true  ),
    LSH     ( "<<"  , false ),
    RSH     ( ">>"  , false ),
    USH     ( ">>>" , false ),
    BWNOT   ( "~"   , false ),
    BWAND   ( "&"   , false ),
    XOR     ( "^"   , false ),
    BWOR    ( "|"   , false ),
    NOT     ( "!"   , false ),
    AND     ( "&&"  , false ),
    OR      ( "||"  , false ),
    LT      ( "<"   , false ),
    LTE     ( "<="  , false ),
    GT      ( ">"   , false ),
    GTE     ( ">="  , false ),
    EQ      ( "=="  , false ),
    EQR     ( "===" , false ),
    NE      ( "!="  , false ),
    NER     ( "!==" , false ),
    INCR    ( "++"  , false ),
    DECR    ( "--"  , false );

    public final String symbol;
    public final boolean exact;

    Operation(final String symbol, final boolean exact) {
        this.symbol = symbol;
        this.exact = exact;
    }
}

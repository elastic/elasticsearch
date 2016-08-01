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

import org.apache.lucene.search.Scorer;
import org.elasticsearch.search.lookup.LeafDocLookup;

import java.util.BitSet;
import java.util.Map;

/**
 * The superclass used to build all Painless scripts on top of.
 */
public abstract class Executable {

    private final String name;
    private final String source;
    private final BitSet statements;

    public Executable(String name, String source, BitSet statements) {
        this.name = name;
        this.source = source;
        this.statements = statements;
    }

    public String getName() {
        return name;
    }

    public String getSource() {
        return source;
    }

    /** 
     * Finds the start of the first statement boundary that is
     * on or before {@code offset}. If one is not found, {@code -1}
     * is returned.
     */
    public int getPreviousStatement(int offset) {
        return statements.previousSetBit(offset);
    }
    
    /** 
     * Finds the start of the first statement boundary that is
     * after {@code offset}. If one is not found, {@code -1}
     * is returned.
     */
    public int getNextStatement(int offset) {
        return statements.nextSetBit(offset+1);
    }

    public abstract Object execute(
        final Map<String, Object> params, final Scorer scorer, final LeafDocLookup doc, final Object value);
}

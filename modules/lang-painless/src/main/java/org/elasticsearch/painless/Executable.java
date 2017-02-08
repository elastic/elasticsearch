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

import java.util.BitSet;
import java.util.Set;

/**
 * The superclass used to build all Painless scripts on top of.
 */
public abstract class Executable implements PainlessScript { // NOCOMMIT rename class
    private final String name;
    private final String source;
    private final BitSet statements;
    private final Set<String> usedVariables;

    public Executable(String name, String source, BitSet statements, Set<String> usedVariables) {
        this.name = name;
        this.source = source;
        this.statements = statements;
        this.usedVariables = usedVariables;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public int getPreviousStatement(int offset) {
        return statements.previousSetBit(offset);
    }

    public int getNextStatement(int offset) {
        return statements.nextSetBit(offset+1);
    }

    @Override
    public Set<String> getUsedVariables() {
        return usedVariables;
    }
}

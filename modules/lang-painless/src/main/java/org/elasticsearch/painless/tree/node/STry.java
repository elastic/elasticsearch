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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.tree.utility.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

public class STry extends Statement {
    protected final Statement block;
    protected final List<STrap> traps;

    public STry(final String location, final Statement block, final List<STrap> traps) {
        super(location);

        this.block = block;
        this.traps = Collections.unmodifiableList(traps);
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        block.lastSource = lastSource;
        block.inLoop = inLoop;
        block.lastLoop = lastLoop;

        variables.incrementScope();
        block.analyze(settings, definition, variables);
        variables.decrementScope();

        methodEscape = block.methodEscape;
        loopEscape = block.loopEscape;
        allEscape = block.allEscape;
        anyContinue = block.anyContinue;
        anyBreak = block.anyBreak;

        int statementCount = 0;

        for (final STrap trap : traps) {
            trap.lastSource = lastSource;
            trap.inLoop = inLoop;
            trap.lastLoop = lastLoop;

            variables.incrementScope();
            trap.analyze(settings, definition, variables);
            variables.decrementScope();

            methodEscape &= trap.methodEscape;
            loopEscape &= trap.loopEscape;
            allEscape &= trap.allEscape;
            anyContinue |= trap.anyContinue;
            anyBreak |= trap.anyBreak;

            statementCount = Math.max(statementCount, trap.statementCount);
        }

        this.statementCount = block.statementCount + statementCount;
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}

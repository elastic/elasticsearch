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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

import java.util.Collections;
import java.util.List;

/**
 * Represents the try block as part of a try-catch block.
 */
public final class STry extends AStatement {

    final AStatement block;
    final List<STrap> traps;

    public STry(final int line, final String location, final AStatement block, final List<STrap> traps) {
        super(line, location);

        this.block = block;
        this.traps = Collections.unmodifiableList(traps);
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
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
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        writeDebugInfo(adapter);
        final Label begin = new Label();
        final Label end = new Label();
        final Label exception = new Label();

        adapter.mark(begin);

        block.continu = continu;
        block.brake = brake;
        block.write(settings, definition, adapter);

        if (!block.allEscape) {
            adapter.goTo(exception);
        }

        adapter.mark(end);

        for (final STrap trap : traps) {
            trap.begin = begin;
            trap.end = end;
            trap.exception = traps.size() > 1 ? exception : null;
            trap.write(settings, definition, adapter);
        }

        if (!block.allEscape || traps.size() > 1) {
            adapter.mark(exception);
        }
    }
}

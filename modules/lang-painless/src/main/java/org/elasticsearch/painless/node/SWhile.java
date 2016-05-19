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

/**
 * Represents a while loop.
 */
public final class SWhile extends AStatement {

    AExpression condition;
    final AStatement block;

    public SWhile(final int line, final String location, final AExpression condition, final AStatement block) {
        super(line, location);

        this.condition = condition;
        this.block = block;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variables.incrementScope();

        condition.expected = definition.getType("boolean");
        condition.analyze(settings, definition, variables);
        condition = condition.cast(settings, definition, variables);

        boolean continuous = false;

        if (condition.constant != null) {
            continuous = (boolean)condition.constant;

            if (!continuous) {
                throw new IllegalArgumentException(error("Extraneous while loop."));
            }

            if (block == null) {
                throw new IllegalArgumentException(error("While loop has no escape."));
            }
        }

        int count = 1;

        if (block != null) {
            block.beginLoop = true;
            block.inLoop = true;

            block.analyze(settings, definition, variables);

            if (block.loopEscape && !block.anyContinue) {
                throw new IllegalArgumentException(error("Extranous while loop."));
            }

            if (continuous && !block.anyBreak) {
                methodEscape = true;
                allEscape = true;
            }

            block.statementCount = Math.max(count, block.statementCount);
        }

        statementCount = 1;

        if (settings.getMaxLoopCounter() > 0) {
            loopCounterSlot = variables.getVariable(location, "#loop").slot;
        }

        variables.decrementScope();
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        writeDebugInfo(adapter);
        final Label begin = new Label();
        final Label end = new Label();

        adapter.mark(begin);

        condition.fals = end;
        condition.write(settings, definition, adapter);

        if (block != null) {
            adapter.writeLoopCounter(loopCounterSlot, Math.max(1, block.statementCount));

            block.continu = begin;
            block.brake = end;
            block.write(settings, definition, adapter);
        } else {
            adapter.writeLoopCounter(loopCounterSlot, 1);
        }

        if (block == null || !block.allEscape) {
            adapter.goTo(begin);
        }

        adapter.mark(end);
    }
}

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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a do-while loop.
 */
public final class SDo extends AStatement {

    final AStatement block;
    AExpression condition;
    final int maxLoopCounter;

    public SDo(int line, String location, AStatement block, AExpression condition, int maxLoopCounter) {
        super(line, location);

        this.condition = condition;
        this.block = block;
        this.maxLoopCounter = maxLoopCounter;
    }

    @Override
    void analyze(Variables variables) {
        variables.incrementScope();

        block.beginLoop = true;
        block.inLoop = true;

        block.analyze(variables);

        if (block.loopEscape && !block.anyContinue) {
            throw new IllegalArgumentException(error("Extraneous do while loop."));
        }

        condition.expected = Definition.BOOLEAN_TYPE;
        condition.analyze(variables);
        condition = condition.cast(variables);

        if (condition.constant != null) {
            final boolean continuous = (boolean)condition.constant;

            if (!continuous) {
                throw new IllegalArgumentException(error("Extraneous do while loop."));
            }

            if (!block.anyBreak) {
                methodEscape = true;
                allEscape = true;
            }
        }

        statementCount = 1;

        if (maxLoopCounter > 0) {
            loopCounterSlot = variables.getVariable(location, "#loop").slot;
        }

        variables.decrementScope();
    }

    @Override
    void write(MethodWriter adapter) {
        writeDebugInfo(adapter);
        final Label start = new Label();
        final Label begin = new Label();
        final Label end = new Label();

        adapter.mark(start);

        block.continu = begin;
        block.brake = end;
        block.write(adapter);

        adapter.mark(begin);

        condition.fals = end;
        condition.write(adapter);

        adapter.writeLoopCounter(loopCounterSlot, Math.max(1, block.statementCount));

        adapter.goTo(start);
        adapter.mark(end);
    }
}

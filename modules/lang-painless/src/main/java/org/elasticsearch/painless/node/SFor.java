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
import org.elasticsearch.painless.antlr.Variables;
import org.elasticsearch.painless.WriterUtility;
import org.objectweb.asm.Label;
import org.objectweb.asm.commons.GeneratorAdapter;

public class SFor extends AStatement {
    protected ANode initializer;
    protected AExpression condition;
    protected AExpression afterthought;
    protected final AStatement block;

    public SFor(final String location,
                final ANode initializer, final AExpression condition, final AExpression afterthought, final AStatement block) {
        super(location);

        this.initializer = initializer;
        this.condition = condition;
        this.afterthought = afterthought;
        this.block = block;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variables.incrementScope();

        boolean continuous = false;

        if (initializer != null) {
            if (initializer instanceof SDeclBlock) {
                ((SDeclBlock)initializer).analyze(settings, definition, variables);
            } else if (initializer instanceof AExpression) {
                final AExpression initializer = (AExpression)this.initializer;

                initializer.read = false;
                initializer.analyze(settings, definition, variables);

                if (!initializer.statement) {
                    throw new IllegalArgumentException(initializer.error("Not a statement."));
                }
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        if (condition != null) {

            condition.expected = definition.booleanType;
            condition.analyze(settings, definition, variables);
            condition = condition.cast(settings, definition, variables);

            if (condition.constant != null) {
                continuous = (boolean)condition.constant;

                if (!continuous) {
                    throw new IllegalArgumentException(error("Extraneous for loop."));
                }

                if (block == null) {
                    throw new IllegalArgumentException(error("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        if (afterthought != null) {
            afterthought.read = false;
            afterthought.analyze(settings, definition, variables);

            if (!afterthought.statement) {
                throw new IllegalArgumentException(afterthought.error("Not a statement."));
            }
        }

        int count = 1;

        if (block != null) {
            block.beginLoop = true;
            block.inLoop = true;

            block.analyze(settings, definition, variables);

            if (block.loopEscape && !block.anyContinue) {
                throw new IllegalArgumentException(error("Extraneous for loop."));
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
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final Label start = new Label();
        final Label begin = afterthought == null ? start : new Label();
        final Label end = new Label();

        if (initializer instanceof SDeclBlock) {
            ((SDeclBlock)initializer).write(settings, definition, adapter);
        } else if (initializer instanceof AExpression) {
            AExpression initializer = (AExpression)this.initializer;

            initializer.write(settings, definition, adapter);
            WriterUtility.writePop(adapter, initializer.expected.sort.size);
        }

        adapter.mark(start);

        if (condition != null) {
            condition.fals = end;
            condition.write(settings, definition, adapter);
        }

        boolean allEscape = false;

        if (block != null) {
            allEscape = block.allEscape;

            int statementCount = Math.max(1, block.statementCount);

            if (afterthought != null) {
                ++statementCount;
            }

            WriterUtility.writeLoopCounter(adapter, loopCounterSlot, statementCount);
            block.write(settings, definition, adapter);
        } else {
            WriterUtility.writeLoopCounter(adapter, loopCounterSlot, 1);
        }

        if (afterthought != null) {
            adapter.mark(begin);
            afterthought.write(settings, definition, adapter);
        }

        if (afterthought != null || !allEscape) {
            adapter.goTo(start);
        }

        adapter.mark(end);
    }
}

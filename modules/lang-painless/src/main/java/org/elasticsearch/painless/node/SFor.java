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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a for loop.
 */
public final class SFor extends AStatement {

    ANode initializer;
    AExpression condition;
    AExpression afterthought;
    final SBlock block;

    public SFor(Location location, ANode initializer, AExpression condition, AExpression afterthought, SBlock block) {
        super(location);

        this.initializer = initializer;
        this.condition = condition;
        this.afterthought = afterthought;
        this.block = block;
    }

    @Override
    void analyze(Locals locals) {
        locals.incrementScope();

        boolean continuous = false;

        if (initializer != null) {
            if (initializer instanceof AStatement) {
                ((AStatement)initializer).analyze(locals);
            } else if (initializer instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializer;

                initializer.read = false;
                initializer.analyze(locals);

                if (!initializer.statement) {
                    throw createError(new IllegalArgumentException("Not a statement."));
                }
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (condition != null) {
            condition.expected = Definition.BOOLEAN_TYPE;
            condition.analyze(locals);
            condition = condition.cast(locals);

            if (condition.constant != null) {
                continuous = (boolean)condition.constant;

                if (!continuous) {
                    throw createError(new IllegalArgumentException("Extraneous for loop."));
                }

                if (block == null) {
                    throw createError(new IllegalArgumentException("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        if (afterthought != null) {
            afterthought.read = false;
            afterthought.analyze(locals);

            if (!afterthought.statement) {
                throw createError(new IllegalArgumentException("Not a statement."));
            }
        }

        if (block != null) {
            block.beginLoop = true;
            block.inLoop = true;

            block.analyze(locals);

            if (block.loopEscape && !block.anyContinue) {
                throw createError(new IllegalArgumentException("Extraneous for loop."));
            }

            if (continuous && !block.anyBreak) {
                methodEscape = true;
                allEscape = true;
            }

            block.statementCount = Math.max(1, block.statementCount);
        }

        statementCount = 1;

        if (locals.getMaxLoopCounter() > 0) {
            loopCounterSlot = locals.getVariable(location, "#loop").slot;
        }

        locals.decrementScope();
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeStatementOffset(location);

        Label start = new Label();
        Label begin = afterthought == null ? start : new Label();
        Label end = new Label();

        if (initializer instanceof SDeclBlock) {
            ((SDeclBlock)initializer).write(writer);
        } else if (initializer instanceof AExpression) {
            AExpression initializer = (AExpression)this.initializer;

            initializer.write(writer);
            writer.writePop(initializer.expected.sort.size);
        }

        writer.mark(start);

        if (condition != null) {
            condition.fals = end;
            condition.write(writer);
        }

        boolean allEscape = false;

        if (block != null) {
            allEscape = block.allEscape;

            int statementCount = Math.max(1, block.statementCount);

            if (afterthought != null) {
                ++statementCount;
            }

            writer.writeLoopCounter(loopCounterSlot, statementCount, location);
            block.write(writer);
        } else {
            writer.writeLoopCounter(loopCounterSlot, 1, location);
        }

        if (afterthought != null) {
            writer.mark(begin);
            afterthought.write(writer);
        }

        if (afterthought != null || !allEscape) {
            writer.goTo(start);
        }

        writer.mark(end);
    }
}

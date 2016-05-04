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
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class SFor extends Statement {
    protected Node initializer;
    protected Expression condition;
    protected Expression afterthought;
    protected final Statement block;

    public SFor(final String location,
                final Node initializer, final Expression condition, final Expression afterthought, final Statement block) {
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
                initializer.analyze(settings, definition, variables);
            } else if (initializer instanceof Expression) {
                final Expression initializer = (Expression)this.initializer;

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
            condition = condition.cast(definition);

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

        variables.decrementScope();
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}

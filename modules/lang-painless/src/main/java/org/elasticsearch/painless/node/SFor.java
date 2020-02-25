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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Arrays;

import static java.util.Collections.emptyList;

/**
 * Represents a for loop.
 */
public final class SFor extends AStatement {

    private ANode initializer;
    private AExpression condition;
    private AExpression afterthought;
    private final SBlock block;

    private boolean continuous = false;

    public SFor(Location location, ANode initializer, AExpression condition, AExpression afterthought, SBlock block) {
        super(location);

        this.initializer = initializer;
        this.condition = condition;
        this.afterthought = afterthought;
        this.block = block;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        scope = scope.newLocalScope();

        if (initializer != null) {
            if (initializer instanceof SDeclBlock) {
                ((SDeclBlock)initializer).analyze(scriptRoot, scope);
            } else if (initializer instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializer;

                initializer.read = false;
                initializer.analyze(scriptRoot, scope);

                if (!initializer.statement) {
                    throw createError(new IllegalArgumentException("Not a statement."));
                }

                initializer.expected = initializer.actual;
                this.initializer = initializer.cast(scriptRoot, scope);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (condition != null) {
            condition.expected = boolean.class;
            condition.analyze(scriptRoot, scope);
            condition = condition.cast(scriptRoot, scope);

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
            afterthought.analyze(scriptRoot, scope);

            if (!afterthought.statement) {
                throw createError(new IllegalArgumentException("Not a statement."));
            }

            afterthought.expected = afterthought.actual;
            afterthought = afterthought.cast(scriptRoot, scope);
        }

        if (block != null) {
            block.beginLoop = true;
            block.inLoop = true;

            block.analyze(scriptRoot, scope);

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
    }

    @Override
    ForLoopNode write(ClassNode classNode) {
        ForLoopNode forLoopNode = new ForLoopNode();

        forLoopNode.setInitialzerNode(initializer == null ? null : initializer.write(classNode));
        forLoopNode.setConditionNode(condition == null ? null : condition.write(classNode));
        forLoopNode.setAfterthoughtNode(afterthought == null ? null : afterthought.write(classNode));
        forLoopNode.setBlockNode(block == null ? null : block.write(classNode));

        forLoopNode.setLocation(location);
        forLoopNode.setContinuous(continuous);

        return forLoopNode;
    }

    @Override
    public String toString() {
        return multilineToString(emptyList(), Arrays.asList(initializer, condition, afterthought, block));
    }
}

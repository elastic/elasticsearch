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
import org.elasticsearch.painless.ir.ExpressionNode;
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
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        scope = scope.newLocalScope();

        if (initializer != null) {
            if (initializer instanceof SDeclBlock) {
                ((SDeclBlock)initializer).analyze(scriptRoot, scope, new Input());
            } else if (initializer instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializer;

                AExpression.Input initializerInput = new AExpression.Input();
                initializerInput.read = false;
                AExpression.Output initializerOutput = initializer.analyze(scriptRoot, scope, initializerInput);

                if (initializerOutput.statement == false) {
                    throw createError(new IllegalArgumentException("Not a statement."));
                }

                initializer.input.expected = initializerOutput.actual;
                initializer.cast();
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (condition != null) {
            AExpression.Input conditionInput = new AExpression.Input();
            conditionInput.expected = boolean.class;
            condition.analyze(scriptRoot, scope, conditionInput);
            condition.cast();

            if (condition instanceof EBoolean) {
                continuous = ((EBoolean)condition).constant;

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
            AExpression.Input afterthoughtInput = new AExpression.Input();
            afterthoughtInput.read = false;
            AExpression.Output afterthoughtOutput = afterthought.analyze(scriptRoot, scope, afterthoughtInput);

            if (afterthoughtOutput.statement == false) {
                throw createError(new IllegalArgumentException("Not a statement."));
            }

            afterthought.input.expected = afterthoughtOutput.actual;
            afterthought.cast();
        }

        if (block != null) {
            Input blockInput = new Input();
            blockInput.beginLoop = true;
            blockInput.inLoop = true;

            Output blockOutput = block.analyze(scriptRoot, scope, blockInput);

            if (blockOutput.loopEscape && blockOutput.anyContinue == false) {
                throw createError(new IllegalArgumentException("Extraneous for loop."));
            }

            if (continuous && blockOutput.anyBreak == false) {
                output.methodEscape = true;
                output.allEscape = true;
            }

            blockOutput.statementCount = Math.max(1, blockOutput.statementCount);
        }

        output.statementCount = 1;

        return output;
    }

    @Override
    ForLoopNode write(ClassNode classNode) {
        ForLoopNode forLoopNode = new ForLoopNode();

        forLoopNode.setInitialzerNode(initializer == null ? null : initializer instanceof AExpression ?
                ((AExpression)initializer).cast((ExpressionNode)initializer.write(classNode)) :
                initializer.write(classNode));
        forLoopNode.setConditionNode(condition == null ? null : condition.cast(condition.write(classNode)));
        forLoopNode.setAfterthoughtNode(afterthought == null ? null : afterthought.cast(afterthought.write(classNode)));
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

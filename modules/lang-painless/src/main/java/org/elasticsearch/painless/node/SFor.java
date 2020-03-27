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
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

/**
 * Represents a for loop.
 */
public class SFor extends AStatement {

    protected final ANode initializer;
    protected final AExpression condition;
    protected final AExpression afterthought;
    protected final SBlock block;

    public SFor(Location location, ANode initializer, AExpression condition, AExpression afterthought, SBlock block) {
        super(location);

        this.initializer = initializer;
        this.condition = condition;
        this.afterthought = afterthought;
        this.block = block;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        scope = scope.newLocalScope();

        Output initializerStatementOutput = null;
        AExpression.Output initializerExpressionOutput = null;

        if (initializer != null) {
            if (initializer instanceof SDeclBlock) {
                initializerStatementOutput = ((SDeclBlock)initializer).analyze(classNode, scriptRoot, scope, new Input());
            } else if (initializer instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializer;

                AExpression.Input initializerInput = new AExpression.Input();
                initializerInput.read = false;
                initializerExpressionOutput = initializer.analyze(classNode, scriptRoot, scope, initializerInput);

                initializerInput.expected = initializerExpressionOutput.actual;
                initializer.cast(initializerInput, initializerExpressionOutput);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        boolean continuous = false;

        AExpression.Output conditionOutput = null;

        if (condition != null) {
            AExpression.Input conditionInput = new AExpression.Input();
            conditionInput.expected = boolean.class;
            conditionOutput = condition.analyze(classNode, scriptRoot, scope, conditionInput);
            condition.cast(conditionInput, conditionOutput);

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

        AExpression.Output afterthoughtOutput = null;

        if (afterthought != null) {
            AExpression.Input afterthoughtInput = new AExpression.Input();
            afterthoughtInput.read = false;
            afterthoughtOutput = afterthought.analyze(classNode, scriptRoot, scope, afterthoughtInput);

            afterthoughtInput.expected = afterthoughtOutput.actual;
            afterthought.cast(afterthoughtInput, afterthoughtOutput);
        }

        Output output = new Output();
        Output blockOutput = null;

        if (block != null) {
            Input blockInput = new Input();
            blockInput.beginLoop = true;
            blockInput.inLoop = true;

            blockOutput = block.analyze(classNode, scriptRoot, scope, blockInput);

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

        ForLoopNode forLoopNode = new ForLoopNode();
        forLoopNode.setInitialzerNode(initializer == null ? null : initializer instanceof AExpression ?
                ((AExpression)initializer).cast(initializerExpressionOutput) :
                initializerStatementOutput.statementNode);
        forLoopNode.setConditionNode(condition == null ? null : condition.cast(conditionOutput));
        forLoopNode.setAfterthoughtNode(afterthought == null ? null : afterthought.cast(afterthoughtOutput));
        forLoopNode.setBlockNode(blockOutput == null ? null : (BlockNode)blockOutput.statementNode);
        forLoopNode.setLocation(location);
        forLoopNode.setContinuous(continuous);

        output.statementNode = forLoopNode;

        return output;
    }
}

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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.lookup.PainlessCast;

/**
 * Represents a for loop.
 */
public class SFor extends AStatement {

    private final ANode initializerNode;
    private final AExpression conditionNode;
    private final AExpression afterthoughtNode;
    private final SBlock blockNode;

    public SFor(int identifier, Location location,
            ANode initializerNode, AExpression conditionNode, AExpression afterthoughtNode, SBlock blockNode) {

        super(identifier, location);

        this.initializerNode = initializerNode;
        this.conditionNode = conditionNode;
        this.afterthoughtNode = afterthoughtNode;
        this.blockNode = blockNode;
    }

    public ANode getInitializerNode() {
        return initializerNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getAfterthoughtNode() {
        return afterthoughtNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        semanticScope = semanticScope.newLocalScope();

        Output initializerStatementOutput = null;
        AExpression.Output initializerExpressionOutput = null;

        if (initializerNode != null) {
            if (initializerNode instanceof SDeclBlock) {
                initializerStatementOutput = ((SDeclBlock)initializerNode).analyze(classNode, semanticScope, new Input());
            } else if (initializerNode instanceof AExpression) {
                AExpression initializer = (AExpression)this.initializerNode;

                AExpression.Input initializerInput = new AExpression.Input();
                initializerInput.read = false;
                initializerExpressionOutput = AExpression.analyze(initializer, classNode, semanticScope, initializerInput);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        boolean continuous = false;

        AExpression.Output conditionOutput = null;
        PainlessCast conditionCast = null;

        if (conditionNode != null) {
            AExpression.Input conditionInput = new AExpression.Input();
            conditionInput.expected = boolean.class;
            conditionOutput = AExpression.analyze(conditionNode, classNode, semanticScope, conditionInput);
            conditionCast = AnalyzerCaster.getLegalCast(conditionNode.getLocation(),
                    conditionOutput.actual, conditionInput.expected, conditionInput.explicit, conditionInput.internal);

            if (conditionNode instanceof EBoolean) {
                continuous = ((EBoolean)conditionNode).getBool();

                if (!continuous) {
                    throw createError(new IllegalArgumentException("Extraneous for loop."));
                }

                if (blockNode == null) {
                    throw createError(new IllegalArgumentException("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        AExpression.Output afterthoughtOutput = null;

        if (afterthoughtNode != null) {
            AExpression.Input afterthoughtInput = new AExpression.Input();
            afterthoughtInput.read = false;
            afterthoughtOutput = AExpression.analyze(afterthoughtNode, classNode, semanticScope, afterthoughtInput);
        }

        Output output = new Output();
        Output blockOutput = null;

        if (blockNode != null) {
            Input blockInput = new Input();
            blockInput.beginLoop = true;
            blockInput.inLoop = true;

            blockOutput = blockNode.analyze(classNode, semanticScope, blockInput);

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
        forLoopNode.setInitialzerNode(initializerNode == null ? null : initializerNode instanceof AExpression ?
                initializerExpressionOutput.expressionNode : initializerStatementOutput.statementNode);
        forLoopNode.setConditionNode(conditionOutput == null ?
                null : AExpression.cast(conditionOutput.expressionNode, conditionCast));
        forLoopNode.setAfterthoughtNode(afterthoughtOutput == null ? null : afterthoughtOutput.expressionNode);
        forLoopNode.setBlockNode(blockOutput == null ? null : (BlockNode)blockOutput.statementNode);
        forLoopNode.setLocation(getLocation());
        forLoopNode.setContinuous(continuous);

        output.statementNode = forLoopNode;

        return output;
    }
}

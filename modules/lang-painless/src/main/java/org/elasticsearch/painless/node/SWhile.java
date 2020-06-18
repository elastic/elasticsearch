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
import org.elasticsearch.painless.ir.WhileNode;
import org.elasticsearch.painless.lookup.PainlessCast;

import java.util.Objects;

/**
 * Represents a while loop.
 */
public class SWhile extends AStatement {

    private final AExpression conditionNode;
    private final SBlock blockNode;

    public SWhile(int identifier, Location location, AExpression conditionNode, SBlock blockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.blockNode = blockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();
        semanticScope = semanticScope.newLocalScope();

        AExpression.Input conditionInput = new AExpression.Input();
        conditionInput.expected = boolean.class;
        AExpression.Output conditionOutput = AExpression.analyze(conditionNode, classNode, semanticScope, conditionInput);
        PainlessCast conditionCast = AnalyzerCaster.getLegalCast(conditionNode.getLocation(),
                conditionOutput.actual, conditionInput.expected, conditionInput.explicit, conditionInput.internal);


        boolean continuous = false;

        if (conditionNode instanceof EBoolean) {
            continuous = ((EBoolean)conditionNode).getBool();

            if (!continuous) {
                throw createError(new IllegalArgumentException("Extraneous while loop."));
            }

            if (blockNode == null) {
                throw createError(new IllegalArgumentException("While loop has no escape."));
            }
        }

        Output blockOutput = null;

        if (blockNode != null) {
            Input blockInput = new Input();
            blockInput.beginLoop = true;
            blockInput.inLoop = true;

            blockOutput = blockNode.analyze(classNode, semanticScope, blockInput);

            if (blockOutput.loopEscape && blockOutput.anyContinue == false) {
                throw createError(new IllegalArgumentException("Extraneous while loop."));
            }

            if (continuous && blockOutput.anyBreak == false) {
                output.methodEscape = true;
                output.allEscape = true;
            }

            blockOutput.statementCount = Math.max(1, blockOutput.statementCount);
        }

        output.statementCount = 1;

        WhileNode whileNode = new WhileNode();
        whileNode.setConditionNode(AExpression.cast(conditionOutput.expressionNode, conditionCast));
        whileNode.setBlockNode(blockOutput == null ? null : (BlockNode)blockOutput.statementNode);
        whileNode.setLocation(getLocation());
        whileNode.setContinuous(continuous);

        output.statementNode = whileNode;

        return output;
    }
}

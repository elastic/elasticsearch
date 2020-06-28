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
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.lookup.PainlessCast;

import java.util.Objects;

/**
 * Represents an if block.
 */
public class SIf extends AStatement {

    private final AExpression conditionNode;
    private final SBlock ifblockNode;

    public SIf(int identifier, Location location, AExpression conditionNode, SBlock ifblockNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.ifblockNode = ifblockNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public SBlock getIfblockNode() {
        return ifblockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();

        AExpression.Input conditionInput = new AExpression.Input();
        conditionInput.expected = boolean.class;
        AExpression.Output conditionOutput = AExpression.analyze(conditionNode, classNode, semanticScope, conditionInput);
        PainlessCast conditionCast = AnalyzerCaster.getLegalCast(conditionNode.getLocation(),
                conditionOutput.actual, conditionInput.expected, conditionInput.explicit, conditionInput.internal);

        if (conditionNode instanceof EBoolean) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblockNode == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        Input ifblockInput = new Input();
        ifblockInput.lastSource = input.lastSource;
        ifblockInput.inLoop = input.inLoop;
        ifblockInput.lastLoop = input.lastLoop;

        Output ifblockOutput = ifblockNode.analyze(classNode, semanticScope.newLocalScope(), ifblockInput);

        output.anyContinue = ifblockOutput.anyContinue;
        output.anyBreak = ifblockOutput.anyBreak;
        output.statementCount = ifblockOutput.statementCount;

        IfNode ifNode = new IfNode();
        ifNode.setConditionNode(AExpression.cast(conditionOutput.expressionNode, conditionCast));
        ifNode.setBlockNode((BlockNode)ifblockOutput.statementNode);
        ifNode.setLocation(getLocation());

        output.statementNode = ifNode;

        return output;
    }
}

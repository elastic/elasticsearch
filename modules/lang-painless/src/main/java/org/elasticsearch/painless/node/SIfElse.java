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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents an if/else block.
 */
public class SIfElse extends AStatement {

    protected final AExpression condition;
    protected final SBlock ifblock;
    protected final SBlock elseblock;

    public SIfElse(Location location, AExpression condition, SBlock ifblock, SBlock elseblock) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.ifblock = ifblock;
        this.elseblock = elseblock;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        AExpression.Input conditionInput = new AExpression.Input();
        conditionInput.expected = boolean.class;
        AExpression.Output conditionOutput = AExpression.analyze(condition, classNode, scriptRoot, scope, conditionInput);
        PainlessCast conditionCast = AnalyzerCaster.getLegalCast(condition.location,
                conditionOutput.actual, conditionInput.expected, conditionInput.explicit, conditionInput.internal);


        if (condition.getChildIf(EBoolean.class) != null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblock == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        Input ifblockInput = new Input();
        ifblockInput.lastSource = input.lastSource;
        ifblockInput.inLoop = input.inLoop;
        ifblockInput.lastLoop = input.lastLoop;

        Output ifblockOutput = ifblock.analyze(classNode, scriptRoot, scope.newLocalScope(), ifblockInput);

        output.anyContinue = ifblockOutput.anyContinue;
        output.anyBreak = ifblockOutput.anyBreak;
        output.statementCount = ifblockOutput.statementCount;

        if (elseblock == null) {
            throw createError(new IllegalArgumentException("Extraneous else statement."));
        }

        Input elseblockInput = new Input();
        elseblockInput.lastSource = input.lastSource;
        elseblockInput.inLoop = input.inLoop;
        elseblockInput.lastLoop = input.lastLoop;

        Output elseblockOutput = elseblock.analyze(classNode, scriptRoot, scope.newLocalScope(), elseblockInput);

        output.methodEscape = ifblockOutput.methodEscape && elseblockOutput.methodEscape;
        output.loopEscape = ifblockOutput.loopEscape && elseblockOutput.loopEscape;
        output.allEscape = ifblockOutput.allEscape && elseblockOutput.allEscape;
        output.anyContinue |= elseblockOutput.anyContinue;
        output.anyBreak |= elseblockOutput.anyBreak;
        output.statementCount = Math.max(ifblockOutput.statementCount, elseblockOutput.statementCount);

        IfElseNode ifElseNode = new IfElseNode();
        ifElseNode.setConditionNode(AExpression.cast(conditionOutput.expressionNode, conditionCast));
        ifElseNode.setBlockNode((BlockNode)ifblockOutput.statementNode);
        ifElseNode.setElseBlockNode((BlockNode)elseblockOutput.statementNode);
        ifElseNode.setLocation(location);

        output.statementNode = ifElseNode;

        return output;
    }
}

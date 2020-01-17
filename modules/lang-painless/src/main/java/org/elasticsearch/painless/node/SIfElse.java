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
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Arrays;
import java.util.Objects;

import static java.util.Collections.singleton;

/**
 * Represents an if/else block.
 */
public final class SIfElse extends AStatement {

    private AExpression condition;
    private final SBlock ifblock;
    private final SBlock elseblock;

    public SIfElse(Location location, AExpression condition, SBlock ifblock, SBlock elseblock) {
        super(location);

        this.condition = Objects.requireNonNull(condition);
        this.ifblock = ifblock;
        this.elseblock = elseblock;
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        AExpression.Input conditionInput = new AExpression.Input();
        conditionInput.expected = boolean.class;
        condition.analyze(scriptRoot, scope, conditionInput);
        condition.cast();

        if (condition instanceof EBoolean) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        if (ifblock == null) {
            throw createError(new IllegalArgumentException("Extraneous if statement."));
        }

        Input ifblockInput = new Input();
        ifblockInput.lastSource = input.lastSource;
        ifblockInput.inLoop = input.inLoop;
        ifblockInput.lastLoop = input.lastLoop;

        Output ifblockOutput = ifblock.analyze(scriptRoot, scope.newLocalScope(), ifblockInput);

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

        Output elseblockOutput = elseblock.analyze(scriptRoot, scope.newLocalScope(), elseblockInput);

        output.methodEscape = ifblockOutput.methodEscape && elseblockOutput.methodEscape;
        output.loopEscape = ifblockOutput.loopEscape && elseblockOutput.loopEscape;
        output.allEscape = ifblockOutput.allEscape && elseblockOutput.allEscape;
        output.anyContinue |= elseblockOutput.anyContinue;
        output.anyBreak |= elseblockOutput.anyBreak;
        output.statementCount = Math.max(ifblockOutput.statementCount, elseblockOutput.statementCount);

        return output;
    }

    @Override
    IfElseNode write(ClassNode classNode) {
        IfElseNode ifElseNode = new IfElseNode();

        ifElseNode.setConditionNode(condition.cast(condition.write(classNode)));
        ifElseNode.setBlockNode(ifblock.write(classNode));
        ifElseNode.setElseBlockNode(elseblock.write(classNode));

        ifElseNode.setLocation(location);

        return ifElseNode;
    }

    @Override
    public String toString() {
        return multilineToString(singleton(condition), Arrays.asList(ifblock, elseblock));
    }
}

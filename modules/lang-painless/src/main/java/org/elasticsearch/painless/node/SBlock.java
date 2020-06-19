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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a set of statements as a branch of control-flow.
 */
public class SBlock extends AStatement {

    private final List<AStatement> statementNodes;

    public SBlock(int identifier, Location location, List<AStatement> statementNodes) {
        super(identifier, location);

        this.statementNodes = Collections.unmodifiableList(Objects.requireNonNull(statementNodes));
    }

    public List<AStatement> getStatementNodes() {
        return statementNodes;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();

        if (statementNodes.isEmpty()) {
            throw createError(new IllegalArgumentException("A block must contain at least one statement."));
        }

        AStatement last = statementNodes.get(statementNodes.size() - 1);

        List<Output> statementOutputs = new ArrayList<>(statementNodes.size());

        for (AStatement statement : statementNodes) {
            // Note that we do not need to check after the last statement because
            // there is no statement that can be unreachable after the last.
            if (output.allEscape) {
                throw createError(new IllegalArgumentException("Unreachable statement."));
            }

            Input statementInput = new Input();
            statementInput.inLoop = input.inLoop;
            statementInput.lastSource = input.lastSource && statement == last;
            statementInput.lastLoop = (input.beginLoop || input.lastLoop) && statement == last;

            Output statementOutput = statement.analyze(classNode, semanticScope, statementInput);

            output.methodEscape = statementOutput.methodEscape;
            output.loopEscape = statementOutput.loopEscape;
            output.allEscape = statementOutput.allEscape;
            output.anyContinue |= statementOutput.anyContinue;
            output.anyBreak |= statementOutput.anyBreak;
            output.statementCount += statementOutput.statementCount;

            statementOutputs.add(statementOutput);
        }

        BlockNode blockNode = new BlockNode();

        for (Output statementOutput : statementOutputs) {
            blockNode.addStatementNode(statementOutput.statementNode);
        }

        blockNode.setLocation(getLocation());
        blockNode.setAllEscape(output.allEscape);
        blockNode.setStatementCount(output.statementCount);

        output.statementNode = blockNode;

        return output;
    }
}

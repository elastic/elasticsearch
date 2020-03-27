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
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents the try block as part of a try-catch block.
 */
public class STry extends AStatement {

    protected final SBlock block;
    protected final List<SCatch> catches;

    public STry(Location location, SBlock block, List<SCatch> catches) {
        super(location);

        this.block = block;
        this.catches = Collections.unmodifiableList(catches);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        if (block == null) {
            throw createError(new IllegalArgumentException("Extraneous try statement."));
        }

        Input blockInput = new Input();
        blockInput.lastSource = input.lastSource;
        blockInput.inLoop = input.inLoop;
        blockInput.lastLoop = input.lastLoop;

        Output blockOutput = block.analyze(classNode, scriptRoot, scope.newLocalScope(), blockInput);

        output.methodEscape = blockOutput.methodEscape;
        output.loopEscape = blockOutput.loopEscape;
        output.allEscape = blockOutput.allEscape;
        output.anyContinue = blockOutput.anyContinue;
        output.anyBreak = blockOutput.anyBreak;

        int statementCount = 0;

        List<Output> catchOutputs = new ArrayList<>();

        for (SCatch catc : catches) {
            Input catchInput = new Input();
            catchInput.lastSource = input.lastSource;
            catchInput.inLoop = input.inLoop;
            catchInput.lastLoop = input.lastLoop;

            Output catchOutput = catc.analyze(classNode, scriptRoot, scope.newLocalScope(), catchInput);

            output.methodEscape &= catchOutput.methodEscape;
            output.loopEscape &= catchOutput.loopEscape;
            output.allEscape &= catchOutput.allEscape;
            output.anyContinue |= catchOutput.anyContinue;
            output.anyBreak |= catchOutput.anyBreak;

            catchOutputs.add(catchOutput);

            statementCount = Math.max(statementCount, catchOutput.statementCount);
        }

        output.statementCount = blockOutput.statementCount + statementCount;

        TryNode tryNode = new TryNode();

        for (Output catchOutput : catchOutputs) {
            tryNode.addCatchNode((CatchNode)catchOutput.statementNode);
        }

        tryNode.setBlockNode((BlockNode)blockOutput.statementNode);
        tryNode.setLocation(location);

        output.statementNode = tryNode;

        return output;
    }
}

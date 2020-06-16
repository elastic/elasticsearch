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
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a catch block as part of a try-catch block.
 */
public class SCatch extends AStatement {

    private final Class<?> baseException;
    private final SDeclaration declarationNode;
    private final SBlock blockNode;

    public SCatch(int identifier, Location location, Class<?> baseException, SDeclaration declarationNode, SBlock blockNode) {
        super(identifier, location);

        this.baseException = Objects.requireNonNull(baseException);
        this.declarationNode = Objects.requireNonNull(declarationNode);
        this.blockNode = blockNode;
    }

    public Class<?> getBaseException() {
        return baseException;
    }

    public SDeclaration getDeclarationNode() {
        return declarationNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        Output declarationOutput = declarationNode.analyze(classNode, scriptRoot, scope, new Input());

        Class<?> type = scope.getVariable(getLocation(), declarationNode.getSymbol()).getType();

        if (baseException.isAssignableFrom(type) == false) {
            throw createError(new ClassCastException(
                    "cannot cast from [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] " +
                    "to [" + PainlessLookupUtility.typeToCanonicalTypeName(baseException) + "]"));
        }

        Output blockOutput = null;

        if (blockNode != null) {
            Input blockInput = new Input();
            blockInput.lastSource = input.lastSource;
            blockInput.inLoop = input.inLoop;
            blockInput.lastLoop = input.lastLoop;
            blockOutput = blockNode.analyze(classNode, scriptRoot, scope, blockInput);

            output.methodEscape = blockOutput.methodEscape;
            output.loopEscape = blockOutput.loopEscape;
            output.allEscape = blockOutput.allEscape;
            output.anyContinue = blockOutput.anyContinue;
            output.anyBreak = blockOutput.anyBreak;
            output.statementCount = blockOutput.statementCount;
        }

        CatchNode catchNode = new CatchNode();
        catchNode.setDeclarationNode((DeclarationNode)declarationOutput.statementNode);
        catchNode.setBlockNode(blockOutput == null ? null : (BlockNode)blockOutput.statementNode);
        catchNode.setLocation(getLocation());

        output.statementNode = catchNode;

        return output;
    }
}

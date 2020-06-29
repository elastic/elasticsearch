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
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a catch block as part of a try-catch block.
 */
public class SCatch extends AStatement {

    private final Class<?> baseException;
    private final String canonicalTypeName;
    private final String symbol;
    private final SBlock blockNode;

    public SCatch(int identifier, Location location, Class<?> baseException, String canonicalTypeName, String symbol, SBlock blockNode) {
        super(identifier, location);

        this.baseException = Objects.requireNonNull(baseException);
        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.blockNode = blockNode;
    }

    public Class<?> getBaseException() {
        return baseException;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        ScriptScope scriptScope = semanticScope.getScriptScope();

        Output output = new Output();

        if (scriptScope.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw createError(new IllegalArgumentException("invalid declaration: type [" + symbol + "] cannot be a name"));
        }

        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        semanticScope.defineVariable(getLocation(), type, symbol, false);

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
            blockOutput = blockNode.analyze(classNode, semanticScope, blockInput);

            output.methodEscape = blockOutput.methodEscape;
            output.loopEscape = blockOutput.loopEscape;
            output.allEscape = blockOutput.allEscape;
            output.anyContinue = blockOutput.anyContinue;
            output.anyBreak = blockOutput.anyBreak;
            output.statementCount = blockOutput.statementCount;
        }

        CatchNode catchNode = new CatchNode();
        catchNode.setExceptionType(type);
        catchNode.setSymbol(symbol);
        catchNode.setBlockNode(blockOutput == null ? null : (BlockNode)blockOutput.statementNode);
        catchNode.setLocation(getLocation());

        output.statementNode = catchNode;

        return output;
    }
}

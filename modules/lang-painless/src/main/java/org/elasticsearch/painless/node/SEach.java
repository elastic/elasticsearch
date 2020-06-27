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
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConditionNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a for-each loop and defers to subnodes depending on type.
 */
public class SEach extends AStatement {

    private final String canonicalTypeName;
    private final String symbol;
    private final AExpression iterableNode;
    private final SBlock blockNode;

    public SEach(int identifier, Location location, String canonicalTypeName, String symbol, AExpression iterableNode, SBlock blockNode) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.iterableNode = Objects.requireNonNull(iterableNode);
        this.blockNode = blockNode;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public AExpression getIterableNode() {
        return iterableNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();

        AExpression.Input expressionInput = new AExpression.Input();
        AExpression.Output expressionOutput = AExpression.analyze(iterableNode, classNode, semanticScope, expressionInput);

        Class<?> clazz = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        semanticScope = semanticScope.newLocalScope();
        Variable variable = semanticScope.defineVariable(getLocation(), clazz, symbol, true);

        if (blockNode == null) {
            throw createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        Input blockInput = new Input();
        blockInput.beginLoop = true;
        blockInput.inLoop = true;
        Output blockOutput = blockNode.analyze(classNode, semanticScope, blockInput);
        blockOutput.statementCount = Math.max(1, blockOutput.statementCount);

        if (blockOutput.loopEscape && blockOutput.anyContinue == false) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        ConditionNode conditionNode;

        if (expressionOutput.actual.isArray()) {
            Variable array =
                    semanticScope.defineVariable(getLocation(), expressionOutput.actual, "#array" + getLocation().getOffset(), true);
            Variable index = semanticScope.defineVariable(getLocation(), int.class, "#index" + getLocation().getOffset(), true);
            Class<?> indexed = expressionOutput.actual.getComponentType();
            PainlessCast cast = AnalyzerCaster.getLegalCast(getLocation(), indexed, variable.getType(), true, true);

            ForEachSubArrayNode forEachSubArrayNode = new ForEachSubArrayNode();
            forEachSubArrayNode.setConditionNode(expressionOutput.expressionNode);
            forEachSubArrayNode.setBlockNode((BlockNode)blockOutput.statementNode);
            forEachSubArrayNode.setLocation(getLocation());
            forEachSubArrayNode.setVariableType(variable.getType());
            forEachSubArrayNode.setVariableName(variable.getName());
            forEachSubArrayNode.setCast(cast);
            forEachSubArrayNode.setArrayType(array.getType());
            forEachSubArrayNode.setArrayName(array.getName());
            forEachSubArrayNode.setIndexType(index.getType());
            forEachSubArrayNode.setIndexName(index.getName());
            forEachSubArrayNode.setIndexedType(indexed);
            forEachSubArrayNode.setContinuous(false);
            conditionNode = forEachSubArrayNode;
        } else if (expressionOutput.actual == def.class || Iterable.class.isAssignableFrom(expressionOutput.actual)) {
            // We must store the iterator as a variable for securing a slot on the stack, and
            // also add the location offset to make the name unique in case of nested for each loops.
            Variable iterator = semanticScope.defineVariable(getLocation(), Iterator.class, "#itr" + getLocation().getOffset(), true);

            PainlessMethod method;

            if (expressionOutput.actual == def.class) {
                method = null;
            } else {
                method = semanticScope.getScriptScope().getPainlessLookup().
                        lookupPainlessMethod(expressionOutput.actual, false, "iterator", 0);

                if (method == null) {
                    throw createError(new IllegalArgumentException(
                            "method [" + typeToCanonicalTypeName(expressionOutput.actual) + ", iterator/0] not found"));
                }
            }

            PainlessCast cast = AnalyzerCaster.getLegalCast(getLocation(), def.class, variable.getType(), true, true);

            ForEachSubIterableNode forEachSubIterableNode = new ForEachSubIterableNode();
            forEachSubIterableNode.setConditionNode(expressionOutput.expressionNode);
            forEachSubIterableNode.setBlockNode((BlockNode)blockOutput.statementNode);
            forEachSubIterableNode.setLocation(getLocation());
            forEachSubIterableNode.setVariableType(variable.getType());
            forEachSubIterableNode.setVariableName(variable.getName());
            forEachSubIterableNode.setCast(cast);
            forEachSubIterableNode.setIteratorType(iterator.getType());
            forEachSubIterableNode.setIteratorName(iterator.getName());
            forEachSubIterableNode.setMethod(method);
            forEachSubIterableNode.setContinuous(false);
            conditionNode = forEachSubIterableNode;
        } else {
            throw createError(new IllegalArgumentException("Illegal for each type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(expressionOutput.actual) + "]."));
        }

        output.statementCount = 1;

        ForEachLoopNode forEachLoopNode = new ForEachLoopNode();
        forEachLoopNode.setConditionNode(conditionNode);
        forEachLoopNode.setLocation(getLocation());

        output.statementNode = forEachLoopNode;

        return output;

    }
}

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
import org.elasticsearch.painless.Scope.Variable;
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
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a for-each loop and defers to subnodes depending on type.
 */
public class SEach extends AStatement {

    protected final String type;
    protected final String name;
    protected final AExpression expression;
    protected final SBlock block;

    public SEach(Location location, String type, String name, AExpression expression, SBlock block) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.expression = Objects.requireNonNull(expression);
        this.block = block;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        AExpression.Input expressionInput = new AExpression.Input();
        AExpression.Output expressionOutput = AExpression.analyze(expression, classNode, scriptRoot, scope, expressionInput);

        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        scope = scope.newLocalScope();
        Variable variable = scope.defineVariable(location, clazz, name, true);

        if (block == null) {
            throw createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        Input blockInput = new Input();
        blockInput.beginLoop = true;
        blockInput.inLoop = true;
        Output blockOutput = block.analyze(classNode, scriptRoot, scope, blockInput);
        blockOutput.statementCount = Math.max(1, blockOutput.statementCount);

        if (blockOutput.loopEscape && blockOutput.anyContinue == false) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        ConditionNode conditionNode;

        if (expressionOutput.actual.isArray()) {
            Variable array = scope.defineVariable(location, expressionOutput.actual, "#array" + location.getOffset(), true);
            Variable index = scope.defineVariable(location, int.class, "#index" + location.getOffset(), true);
            Class<?> indexed = expressionOutput.actual.getComponentType();
            PainlessCast cast = AnalyzerCaster.getLegalCast(location, indexed, variable.getType(), true, true);

            ForEachSubArrayNode forEachSubArrayNode = new ForEachSubArrayNode();
            forEachSubArrayNode.setConditionNode(expressionOutput.expressionNode);
            forEachSubArrayNode.setBlockNode((BlockNode)blockOutput.statementNode);
            forEachSubArrayNode.setLocation(location);
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
            Variable iterator = scope.defineVariable(location, Iterator.class, "#itr" + location.getOffset(), true);

            PainlessMethod method;

            if (expressionOutput.actual == def.class) {
                method = null;
            } else {
                method = scriptRoot.getPainlessLookup().lookupPainlessMethod(expressionOutput.actual, false, "iterator", 0);

                if (method == null) {
                    throw createError(new IllegalArgumentException(
                            "method [" + typeToCanonicalTypeName(expressionOutput.actual) + ", iterator/0] not found"));
                }
            }

            PainlessCast cast = AnalyzerCaster.getLegalCast(location, def.class, variable.getType(), true, true);

            ForEachSubIterableNode forEachSubIterableNode = new ForEachSubIterableNode();
            forEachSubIterableNode.setConditionNode(expressionOutput.expressionNode);
            forEachSubIterableNode.setBlockNode((BlockNode)blockOutput.statementNode);
            forEachSubIterableNode.setLocation(location);
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
        forEachLoopNode.setLocation(location);

        output.statementNode = forEachLoopNode;

        return output;

    }
}

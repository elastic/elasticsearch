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
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConditionNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

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
        AExpression.Output expressionOutput = expression.analyze(classNode, scriptRoot, scope, expressionInput);
        // TODO: no need to cast here
        expressionInput.expected = expressionOutput.actual;
        expression.cast(expressionInput, expressionOutput);

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

        AStatement sub;

        if (expressionOutput.actual.isArray()) {
            sub = new SSubEachArray(location, variable, expressionOutput, blockOutput);
        } else if (expressionOutput.actual == def.class || Iterable.class.isAssignableFrom(expressionOutput.actual)) {
            sub = new SSubEachIterable(location, variable, expressionOutput, blockOutput);
        } else {
            throw createError(new IllegalArgumentException("Illegal for each type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(expressionOutput.actual) + "]."));
        }

        Output subOutput = sub.analyze(classNode, scriptRoot, scope, input);

        output.statementCount = 1;

        ForEachLoopNode forEachLoopNode = new ForEachLoopNode();
        forEachLoopNode.setConditionNode((ConditionNode)subOutput.statementNode);
        forEachLoopNode.setLocation(location);

        output.statementNode = forEachLoopNode;

        return output;

    }
}

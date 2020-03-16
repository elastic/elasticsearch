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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a for-each loop for arrays.
 */
final class SSubEachArray extends AStatement {
    private final Variable variable;
    private AExpression expression;
    private final SBlock block;

    private PainlessCast cast = null;
    private Variable array = null;
    private Variable index = null;
    private Class<?> indexed = null;

    SSubEachArray(Location location, Variable variable, AExpression expression, SBlock block) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.expression = Objects.requireNonNull(expression);
        this.block = block;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        // We must store the array and index as variables for securing slots on the stack, and
        // also add the location offset to make the names unique in case of nested for each loops.
        array = scope.defineInternalVariable(location, expression.actual, "array" + location.getOffset(), true);
        index = scope.defineInternalVariable(location, int.class, "index" + location.getOffset(), true);
        indexed = expression.actual.getComponentType();
        cast = AnalyzerCaster.getLegalCast(location, indexed, variable.getType(), true, true);
    }

    @Override
    ForEachSubArrayNode write(ClassNode classNode) {
        ForEachSubArrayNode forEachSubArrayNode = new ForEachSubArrayNode();

        forEachSubArrayNode.setConditionNode(expression.write(classNode));
        forEachSubArrayNode.setBlockNode(block.write(classNode));

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

        return forEachSubArrayNode;
    }

    @Override
    public String toString() {
        return singleLineToString(variable.getCanonicalTypeName(), variable.getName(), expression, block);
    }
}

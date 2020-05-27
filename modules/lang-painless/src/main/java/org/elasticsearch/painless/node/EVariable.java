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
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a variable load/store.
 */
public class EVariable extends AExpression {

    protected final String name;

    public EVariable(Location location, String name) {
        super(location);

        this.name = Objects.requireNonNull(name);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();
        Class<?> type = scriptRoot.getPainlessLookup().canonicalTypeNameToType(name);

        if (type != null)  {
            if (input.write) {
                throw createError(new IllegalArgumentException("invalid assignment: " +
                        "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "]"));
            }

            if (input.read == false) {
                throw createError(new IllegalArgumentException("not a statement: " +
                        "static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] not used"));
            }

            output.actual = type;
            output.isStaticType = true;

            StaticNode staticNode = new StaticNode();

            staticNode.setLocation(location);
            staticNode.setExpressionType(output.actual);

            output.expressionNode = staticNode;
        } else if (scope.isVariableDefined(name)) {
            if (input.read == false && input.write == false) {
                throw createError(new IllegalArgumentException("not a statement: variable [" + name + "] not used"));
            }

            Variable variable = scope.getVariable(location, name);

            if (input.write && variable.isFinal()) {
                throw createError(new IllegalArgumentException("Variable [" + variable.getName() + "] is read-only."));
            }

            output.actual = variable.getType();

            VariableNode variableNode = new VariableNode();

            variableNode.setLocation(location);
            variableNode.setExpressionType(output.actual);
            variableNode.setName(name);

            output.expressionNode = variableNode;
        } else {
            output.partialCanonicalTypeName = name;
        }

        return output;
    }
}

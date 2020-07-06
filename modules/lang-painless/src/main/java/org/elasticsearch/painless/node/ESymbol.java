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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Objects;

/**
 * Represents a variable load/store.
 */
public class ESymbol extends AExpression {

    private final String symbol;

    public ESymbol(int identifer, Location location, String symbol) {
        super(identifer, location);

        this.symbol = Objects.requireNonNull(symbol);
    }

    public String getSymbol() {
        return symbol;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitSymbol(this, input);
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        Output output = new Output();
        Class<?> staticType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(symbol);
        boolean read = semanticScope.getCondition(this, Read.class);
        boolean write = semanticScope.getCondition(this, Write.class);

        if (staticType != null)  {
            if (write) {
                throw createError(new IllegalArgumentException("invalid assignment: " +
                        "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(staticType) + "]"));
            }

            if (read == false) {
                throw createError(new IllegalArgumentException("not a statement: " +
                        "static type [" + PainlessLookupUtility.typeToCanonicalTypeName(staticType) + "] not used"));
            }

            semanticScope.putDecoration(this, new StaticType(staticType));

            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(getLocation());
            staticNode.setExpressionType(staticType);
            output.expressionNode = staticNode;
        } else if (semanticScope.isVariableDefined(symbol)) {
            if (read == false && write == false) {
                throw createError(new IllegalArgumentException("not a statement: variable [" + symbol + "] not used"));
            }

            Variable variable = semanticScope.getVariable(getLocation(), symbol);

            if (write && variable.isFinal()) {
                throw createError(new IllegalArgumentException("Variable [" + variable.getName() + "] is read-only."));
            }

            Class<?> valueType = variable.getType();
            semanticScope.putDecoration(this, new ValueType(valueType));

            VariableNode variableNode = new VariableNode();
            variableNode.setLocation(getLocation());
            variableNode.setExpressionType(valueType);
            variableNode.setName(symbol);
            output.expressionNode = variableNode;
        } else {
            semanticScope.putDecoration(this, new PartialCanonicalTypeName(symbol));
        }

        return output;
    }
}

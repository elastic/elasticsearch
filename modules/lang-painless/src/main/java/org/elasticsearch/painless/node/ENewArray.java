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
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an array instantiation.
 */
public class ENewArray extends AExpression {

    private final String canonicalTypeName;
    private final List<AExpression> valueNodes;
    private final boolean isInitializer;

    public ENewArray(int identifier, Location location, String canonicalTypeName, List<AExpression> valueNodes, boolean isInitializer) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
        this.isInitializer = isInitializer;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    public boolean isInitializer() {
        return isInitializer;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitNewArray(this, input);
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to new array"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from new array"));
        }

        Output output = new Output();

        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        List<Output> argumentOutputs = new ArrayList<>();
        List<PainlessCast> argumentCasts = new ArrayList<>();

        for (AExpression expression : valueNodes) {
            semanticScope.setCondition(expression, Read.class);
            semanticScope.putDecoration(expression, new TargetType(isInitializer ? valueType.getComponentType() : int.class));
            semanticScope.setCondition(expression, Internal.class);
            Output expressionOutput = analyze(expression, classNode, semanticScope);
            argumentOutputs.add(expressionOutput);
            argumentCasts.add(expression.cast(semanticScope));
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        NewArrayNode newArrayNode = new NewArrayNode();

        for (int i = 0; i < valueNodes.size(); ++ i) {
            newArrayNode.addArgumentNode(cast(argumentOutputs.get(i).expressionNode, argumentCasts.get(i)));
        }

        newArrayNode.setLocation(getLocation());
        newArrayNode.setExpressionType(valueType);
        newArrayNode.setInitialize(isInitializer);
        output.expressionNode = newArrayNode;

        return output;
    }
}

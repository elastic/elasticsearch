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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.lookup.PainlessCast;

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
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to new array"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from new array"));
        }

        Output output = new Output();

        Class<?> clazz = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        List<Output> argumentOutputs = new ArrayList<>();
        List<PainlessCast> argumentCasts = new ArrayList<>();

        for (AExpression expression : valueNodes) {
            Input expressionInput = new Input();
            expressionInput.expected = isInitializer ? clazz.getComponentType() : int.class;
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, semanticScope, expressionInput);
            argumentOutputs.add(expressionOutput);
            argumentCasts.add(AnalyzerCaster.getLegalCast(expression.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));
        }

        output.actual = clazz;

        NewArrayNode newArrayNode = new NewArrayNode();

        for (int i = 0; i < valueNodes.size(); ++ i) {
            newArrayNode.addArgumentNode(cast(argumentOutputs.get(i).expressionNode, argumentCasts.get(i)));
        }

        newArrayNode.setLocation(getLocation());
        newArrayNode.setExpressionType(output.actual);
        newArrayNode.setInitialize(isInitializer);

        output.expressionNode = newArrayNode;

        return output;
    }
}

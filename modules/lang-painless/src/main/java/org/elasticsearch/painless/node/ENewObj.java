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
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents and object instantiation.
 */
public class ENewObj extends AExpression {

    private final String canonicalTypeName;
    private final List<AExpression> argumentNodes;

    public ENewObj(int identifier, Location location, String canonicalTypeName, List<AExpression> argumentNodes) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.argumentNodes = Collections.unmodifiableList(Objects.requireNonNull(argumentNodes));
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public List<AExpression> getArgumentNodes() {
        return argumentNodes;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment cannot assign a value to new object with constructor " +
                    "[" + canonicalTypeName + "/" + argumentNodes.size() + "]"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();

        Output output = new Output();

        output.actual = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (output.actual == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        PainlessConstructor constructor = scriptScope.getPainlessLookup().lookupPainlessConstructor(output.actual, argumentNodes.size());

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/" + argumentNodes.size() + "] not found"));
        }

        scriptScope.markNonDeterministic(constructor.annotations.containsKey(NonDeterministicAnnotation.class));

        Class<?>[] types = new Class<?>[constructor.typeParameters.size()];
        constructor.typeParameters.toArray(types);

        if (constructor.typeParameters.size() != argumentNodes.size()) {
            throw createError(new IllegalArgumentException(
                    "When calling constructor on type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "] " +
                    "expected [" + constructor.typeParameters.size() + "] arguments, but found [" + argumentNodes.size() + "]."));
        }

        List<Output> argumentOutputs = new ArrayList<>();
        List<PainlessCast> argumentCasts = new ArrayList<>();

        for (int i = 0; i < argumentNodes.size(); ++i) {
            AExpression expression = argumentNodes.get(i);

            Input expressionInput = new Input();
            expressionInput.expected = types[i];
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, semanticScope, expressionInput);
            argumentOutputs.add(expressionOutput);
            argumentCasts.add(AnalyzerCaster.getLegalCast(expression.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));
        }

        NewObjectNode newObjectNode = new NewObjectNode();

        for (int i = 0; i < argumentNodes.size(); ++ i) {
            newObjectNode.addArgumentNode(cast(argumentOutputs.get(i).expressionNode, argumentCasts.get(i)));
        }

        newObjectNode.setLocation(getLocation());
        newObjectNode.setExpressionType(output.actual);
        newObjectNode.setRead(input.read);
        newObjectNode.setConstructor(constructor);

        output.expressionNode = newObjectNode;

        return output;
    }
}

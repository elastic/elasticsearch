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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents and object instantiation.
 */
public class ENewObj extends AExpression {

    protected final String type;
    protected final List<AExpression> arguments;

    public ENewObj(Location location, String type, List<AExpression> arguments) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.arguments = Collections.unmodifiableList(Objects.requireNonNull(arguments));
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment cannot assign a value to new object with constructor [" + type + "/" + arguments.size() + "]"));
        }

        Output output = new Output();

        output.actual = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (output.actual == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        PainlessConstructor constructor = scriptRoot.getPainlessLookup().lookupPainlessConstructor(output.actual, arguments.size());

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/" + arguments.size() + "] not found"));
        }

        scriptRoot.markNonDeterministic(constructor.annotations.containsKey(NonDeterministicAnnotation.class));

        Class<?>[] types = new Class<?>[constructor.typeParameters.size()];
        constructor.typeParameters.toArray(types);

        if (constructor.typeParameters.size() != arguments.size()) {
            throw createError(new IllegalArgumentException(
                    "When calling constructor on type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "] " +
                    "expected [" + constructor.typeParameters.size() + "] arguments, but found [" + arguments.size() + "]."));
        }

        List<Output> argumentOutputs = new ArrayList<>();
        List<PainlessCast> argumentCasts = new ArrayList<>();

        for (int i = 0; i < arguments.size(); ++i) {
            AExpression expression = arguments.get(i);

            Input expressionInput = new Input();
            expressionInput.expected = types[i];
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, scriptRoot, scope, expressionInput);
            argumentOutputs.add(expressionOutput);
            argumentCasts.add(AnalyzerCaster.getLegalCast(expression.location,
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));
        }

        NewObjectNode newObjectNode = new NewObjectNode();

        for (int i = 0; i < arguments.size(); ++ i) {
            newObjectNode.addArgumentNode(cast(argumentOutputs.get(i).expressionNode, argumentCasts.get(i)));
        }

        newObjectNode.setLocation(location);
        newObjectNode.setExpressionType(output.actual);
        newObjectNode.setRead(input.read);
        newObjectNode.setConstructor(constructor);

        output.expressionNode = newObjectNode;

        return output;
    }
}

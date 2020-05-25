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
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an array instantiation.
 */
public class ENewArray extends AExpression {

    protected final String type;
    protected final List<AExpression> arguments;
    protected final boolean initialize;

    public ENewArray(Location location, String type, List<AExpression> arguments, boolean initialize) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.arguments = Collections.unmodifiableList(Objects.requireNonNull(arguments));
        this.initialize = initialize;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to new array"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from new array"));
        }

        Output output = new Output();

        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        List<Output> argumentOutputs = new ArrayList<>();
        List<PainlessCast> argumentCasts = new ArrayList<>();

        for (AExpression expression : arguments) {
            Input expressionInput = new Input();
            expressionInput.expected = initialize ? clazz.getComponentType() : int.class;
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, scriptRoot, scope, expressionInput);
            argumentOutputs.add(expressionOutput);
            argumentCasts.add(AnalyzerCaster.getLegalCast(expression.location,
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));
        }

        output.actual = clazz;

        NewArrayNode newArrayNode = new NewArrayNode();

        for (int i = 0; i < arguments.size(); ++ i) {
            newArrayNode.addArgumentNode(cast(argumentOutputs.get(i).expressionNode, argumentCasts.get(i)));
        }

        newArrayNode.setLocation(location);
        newArrayNode.setExpressionType(output.actual);
        newArrayNode.setInitialize(initialize);

        output.expressionNode = newArrayNode;

        return output;
    }
}

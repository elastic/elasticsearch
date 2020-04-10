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
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a method call.
 */
public class PSubCallInvoke extends AExpression {

    protected final PainlessMethod method;
    protected final Class<?> box;
    protected final List<AExpression> arguments;

    PSubCallInvoke(Location location, PainlessMethod method, Class<?> box, List<AExpression> arguments) {
        super(location);

        this.method = Objects.requireNonNull(method);
        this.box = box;
        this.arguments = Collections.unmodifiableList(Objects.requireNonNull(arguments));
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        List<Output> argumentOutputs = new ArrayList<>();

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            Input expressionInput = new Input();
            expressionInput.expected = method.typeParameters.get(argument);
            expressionInput.internal = true;
            Output expressionOutput = expression.analyze(classNode, scriptRoot, scope, expressionInput);
            expression.cast(expressionInput, expressionOutput);
            argumentOutputs.add(expressionOutput);
        }

        output.actual = method.returnType;

        CallSubNode callSubNode = new CallSubNode();

        for (int argument = 0; argument < arguments.size(); ++ argument) {
            callSubNode.addArgumentNode(arguments.get(argument).cast(argumentOutputs.get(argument)));
        }

        callSubNode.setLocation(location);
        callSubNode.setExpressionType(output.actual);
        callSubNode.setMethod(method);
        callSubNode .setBox(box);

        output.expressionNode = callSubNode;

        return output;
    }
}

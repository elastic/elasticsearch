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
import org.elasticsearch.painless.ir.CallSubDefNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a method call made on a def type. (Internal only.)
 */
public class PSubDefCall extends AExpression {

    protected final String name;
    protected final List<AExpression> arguments;

    PSubDefCall(Location location, String name, List<AExpression> arguments) {
        super(location);

        this.name = Objects.requireNonNull(name);
        this.arguments = Collections.unmodifiableList(Objects.requireNonNull(arguments));
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        List<Output> argumentOutputs = new ArrayList<>(arguments.size());

        for (AExpression argument : arguments) {
            Input expressionInput = new Input();
            expressionInput.internal = true;
            Output expressionOutput = argument.analyze(classNode, scriptRoot, scope, expressionInput);
            argumentOutputs.add(expressionOutput);

            if (expressionOutput.actual == void.class) {
                throw createError(new IllegalArgumentException("Argument(s) cannot be of [void] type when calling method [" + name + "]."));
            }

            expressionInput.expected = expressionOutput.actual;
            argument.cast(expressionInput, expressionOutput);
        }

        Output output = new Output();
        // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
        output.actual = input.expected == null || input.expected == ZonedDateTime.class || input.explicit ? def.class : input.expected;

        CallSubDefNode callSubDefNode = new CallSubDefNode();

        for (int argument = 0; argument < arguments.size(); ++ argument) {
            callSubDefNode.addArgumentNode(arguments.get(argument).cast(argumentOutputs.get(argument)));
        }

        callSubDefNode.setLocation(location);
        callSubDefNode.setExpressionType(output.actual);
        callSubDefNode.setName(name);

        output.expressionNode = callSubDefNode;

        return output;
    }
}

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

import java.util.List;
import java.util.Objects;

/**
 * Represents a method call.
 */
final class PSubCallInvoke extends AExpression {

    private final PainlessMethod method;
    private final Class<?> box;
    private final List<AExpression> arguments;

    PSubCallInvoke(Location location, PainlessMethod method, Class<?> box, List<AExpression> arguments) {
        super(location);

        this.method = Objects.requireNonNull(method);
        this.box = box;
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = method.typeParameters.get(argument);
            expression.internal = true;
            expression.analyze(scriptRoot, scope);
            arguments.set(argument, expression.cast(scriptRoot, scope));
        }

        statement = true;
        actual = method.returnType;
    }

    @Override
    CallSubNode write(ClassNode classNode) {
        CallSubNode callSubNode = new CallSubNode();

        for (AExpression argument : arguments) {
            callSubNode.addArgumentNode(argument.write(classNode));
        }

        callSubNode.setLocation(location);
        callSubNode.setExpressionType(actual);
        callSubNode.setMethod(method);
        callSubNode .setBox(box);

        return callSubNode;
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, method.javaMethod.getName());
    }
}

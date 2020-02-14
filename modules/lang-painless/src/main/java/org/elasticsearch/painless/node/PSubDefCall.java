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
import java.util.List;
import java.util.Objects;

/**
 * Represents a method call made on a def type. (Internal only.)
 */
final class PSubDefCall extends AExpression {

    private final String name;
    private final List<AExpression> arguments;

    private final StringBuilder recipe = new StringBuilder();
    private final List<String> pointers = new ArrayList<>();
    private final List<Class<?>> parameterTypes = new ArrayList<>();

    PSubDefCall(Location location, String name, List<AExpression> arguments) {
        super(location);

        this.name = Objects.requireNonNull(name);
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        parameterTypes.add(Object.class);
        int totalCaptures = 0;

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.internal = true;
            expression.analyze(scriptRoot, scope);

            if (expression.actual == void.class) {
                throw createError(new IllegalArgumentException("Argument(s) cannot be of [void] type when calling method [" + name + "]."));
            }

            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(scriptRoot, scope));
            parameterTypes.add(expression.actual);

            if (expression instanceof ILambda) {
                ILambda lambda = (ILambda) expression;
                pointers.add(lambda.getPointer());
                // encode this parameter as a deferred reference
                char ch = (char) (argument + totalCaptures);
                recipe.append(ch);
                totalCaptures += lambda.getCaptureCount();
                parameterTypes.addAll(lambda.getCaptures());
            }
        }

        // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
        actual = expected == null || expected == ZonedDateTime.class || explicit ? def.class : expected;
    }

    @Override
    CallSubDefNode write(ClassNode classNode) {
        CallSubDefNode callSubDefNode = new CallSubDefNode();

        for (AExpression argument : arguments) {
            callSubDefNode.addArgumentNode(argument.write(classNode));
        }

        callSubDefNode.setLocation(location);
        callSubDefNode.setExpressionType(actual);
        callSubDefNode.setName(name);
        callSubDefNode.setRecipe(recipe.toString());
        callSubDefNode.getPointers().addAll(pointers);
        callSubDefNode.getTypeParameters().addAll(parameterTypes);

        return callSubDefNode;
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, name);
    }
}

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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.List;
import java.util.Objects;

/**
 * Represents an array instantiation.
 */
public final class ENewArray extends AExpression {

    private final String type;
    private final List<AExpression> arguments;
    private final boolean initialize;

    public ENewArray(Location location, String type, List<AExpression> arguments, boolean initialize) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.arguments = Objects.requireNonNull(arguments);
        this.initialize = initialize;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        if (!read) {
             throw createError(new IllegalArgumentException("A newly created array must be read from."));
        }

        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = initialize ? clazz.getComponentType() : int.class;
            expression.internal = true;
            expression.analyze(scriptRoot, scope);
            arguments.set(argument, expression.cast(scriptRoot, scope));
        }

        actual = clazz;
    }

    @Override
    NewArrayNode write(ClassNode classNode) {
        NewArrayNode newArrayNode = new NewArrayNode();

        for (AExpression argument : arguments) {
            newArrayNode.addArgumentNode(argument.write(classNode));
        }

        newArrayNode.setLocation(location);
        newArrayNode.setExpressionType(actual);
        newArrayNode.setInitialize(initialize);

        return newArrayNode;
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, type, initialize ? "init" : "dims");
    }
}

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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents and object instantiation.
 */
public final class ENewObj extends AExpression {

    private final String type;
    private final List<AExpression> arguments;

    private Method constructor;

    public ENewObj(Location location, String type, List<AExpression> arguments) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        final Type type;

        try {
            type = locals.getDefinition().getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        Struct struct = type.struct;
        constructor = struct.constructors.get(new Definition.MethodKey("<init>", arguments.size()));

        if (constructor != null) {
            Type[] types = new Type[constructor.arguments.size()];
            constructor.arguments.toArray(types);

            if (constructor.arguments.size() != arguments.size()) {
                throw createError(new IllegalArgumentException("When calling constructor on type [" + struct.name + "]" +
                    " expected [" + constructor.arguments.size() + "] arguments, but found [" + arguments.size() + "]."));
            }

            for (int argument = 0; argument < arguments.size(); ++argument) {
                AExpression expression = arguments.get(argument);

                expression.expected = types[argument];
                expression.internal = true;
                expression.analyze(locals);
                arguments.set(argument, expression.cast(locals));
            }

            statement = true;
            actual = type;
        } else {
            throw createError(new IllegalArgumentException("Unknown new call on type [" + struct.name + "]."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.newInstance(actual.type);

        if (read) {
            writer.dup();
        }

        for (AExpression argument : arguments) {
            argument.write(writer, globals);
        }

        writer.invokeConstructor(constructor.owner.type, constructor.method);
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, type);
    }
}

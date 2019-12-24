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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;

import java.util.List;
import java.util.Objects;
import java.util.Set;

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
    void extractVariables(Set<String> variables) {
        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
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
            expression.analyze(scriptRoot, locals);
            arguments.set(argument, expression.cast(scriptRoot, locals));
        }

        actual = clazz;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        if (initialize) {
            methodWriter.push(arguments.size());
            methodWriter.newArray(MethodWriter.getType(actual.getComponentType()));

            for (int index = 0; index < arguments.size(); ++index) {
                AExpression argument = arguments.get(index);

                methodWriter.dup();
                methodWriter.push(index);
                argument.write(classWriter, methodWriter, globals);
                methodWriter.arrayStore(MethodWriter.getType(actual.getComponentType()));
            }
        } else {
            for (AExpression argument : arguments) {
                argument.write(classWriter, methodWriter, globals);
            }

            if (arguments.size() > 1) {
                methodWriter.visitMultiANewArrayInsn(MethodWriter.getType(actual).getDescriptor(), arguments.size());
            } else {
                methodWriter.newArray(MethodWriter.getType(actual.getComponentType()));
            }
        }
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, type, initialize ? "init" : "dims");
    }
}

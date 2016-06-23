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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an array instantiation.
 */
public final class LNewArray extends ALink {

    final String type;
    final List<AExpression> arguments;
    final boolean initialize;

    public LNewArray(Location location, String type, List<AExpression> arguments, boolean initialize) {
        super(location, -1);

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
    ALink analyze(Locals locals) {
        if (before != null) {
            throw createError(new IllegalArgumentException("Cannot create a new array with a target already defined."));
        } else if (store) {
            throw createError(new IllegalArgumentException("Cannot assign a value to a new array."));
        } else if (!load) {
            throw createError(new IllegalArgumentException("A newly created array must be read."));
        }

        final Type type;

        try {
            type = Definition.getType(this.type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = initialize ? Definition.getType(type.struct, 0) : Definition.INT_TYPE;
            expression.internal = true;
            expression.analyze(locals);
            arguments.set(argument, expression.cast(locals));
        }

        after = Definition.getType(type.struct, initialize ? 1 : arguments.size());

        return this;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (initialize) {
            writer.push(arguments.size());
            writer.newArray(Definition.getType(after.struct, 0).type);

            for (int index = 0; index < arguments.size(); ++index) {
                AExpression argument = arguments.get(index);

                writer.dup();
                writer.push(index);
                argument.write(writer, globals);
                writer.arrayStore(Definition.getType(after.struct, 0).type);
            }
        } else {
            for (AExpression argument : arguments) {
                argument.write(writer, globals);
            }

            if (arguments.size() > 1) {
                writer.visitMultiANewArrayInsn(after.type.getDescriptor(), after.type.getDimensions());
            } else {
                writer.newArray(Definition.getType(after.struct, 0).type);
            }
        }
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}

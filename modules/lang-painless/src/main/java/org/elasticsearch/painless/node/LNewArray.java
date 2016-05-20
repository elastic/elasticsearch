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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;

/**
 * Represents an array instantiation.
 */
public final class LNewArray extends ALink {

    final String type;
    final List<AExpression> arguments;

    public LNewArray(final int line, final String location, final String type, final List<AExpression> arguments) {
        super(line, location, -1);

        this.type = type;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(final CompilerSettings settings, final Variables variables) {
        if (before != null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a new array."));
        } else if (!load) {
            throw new IllegalArgumentException(error("A newly created array must be assigned."));
        }

        final Type type;

        try {
            type = Definition.getType(this.type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + this.type + "]."));
        }

        for (int argument = 0; argument < arguments.size(); ++argument) {
            final AExpression expression = arguments.get(argument);

            expression.expected = Definition.INT_TYPE;
            expression.analyze(settings, variables);
            arguments.set(argument, expression.cast(settings, variables));
        }

        after = Definition.getType(type.struct, arguments.size());

        return this;
    }

    @Override
    void write(final CompilerSettings settings, final MethodWriter adapter) {
        // Do nothing.
    }

    @Override
    void load(final CompilerSettings settings, final MethodWriter adapter) {
        for (final AExpression argument : arguments) {
            argument.write(settings, adapter);
        }

        if (arguments.size() > 1) {
            adapter.visitMultiANewArrayInsn(after.type.getDescriptor(), after.type.getDimensions());
        } else {
            adapter.newArray(Definition.getType(after.struct, 0).type);
        }
    }

    @Override
    void store(final CompilerSettings settings, final MethodWriter adapter) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}

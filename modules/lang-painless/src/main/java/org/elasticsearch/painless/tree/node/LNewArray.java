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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

public class LNewArray extends ALink {
    protected final String type;
    protected final List<AExpression> arguments;

    public LNewArray(final String location, final String type, final List<AExpression> arguments) {
        super(location);

        this.type = type;
        this.arguments = Collections.unmodifiableList(arguments);
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before != null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a new array."));
        } else if (!load) {
            throw new IllegalArgumentException(error("A newly created array must be assigned."));
        }

        final Type type;

        try {
            type = definition.getType(this.type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + this.type + "]."));
        }

        for (int argument = 0; argument < arguments.size(); ++argument) {
            final AExpression expression = arguments.get(argument);

            expression.expected = definition.intType;
            expression.analyze(settings, definition, variables);
            arguments.set(argument, expression.cast(settings, definition, variables));
        }

        after = definition.getType(type.struct, arguments.size());

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (begincat) {
            Shared.writeNewStrings(adapter);
        }

        for (final AExpression argument : arguments) {
            argument.write(settings, definition, adapter);
        }

        if (arguments.size() > 1) {
            adapter.visitMultiANewArrayInsn(after.type.getDescriptor(), after.type.getDimensions());
        } else {
            adapter.newArray(after.type);
        }
    }
}

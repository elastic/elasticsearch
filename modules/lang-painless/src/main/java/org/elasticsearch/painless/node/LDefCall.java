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
import org.elasticsearch.painless.DynamicCallSite;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

import static org.elasticsearch.painless.WriterConstants.DEF_BOOTSTRAP_HANDLE;

public class LDefCall extends ALink {
    protected final String name;
    protected final List<AExpression> arguments;

    protected LDefCall(final String location, final String name, final List<AExpression> arguments) {
        super(location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        for (int argument = 0; argument < arguments.size(); ++argument) {
            final AExpression expression = arguments.get(argument);

            expression.analyze(settings, definition, variables);
            expression.expected = expression.actual;
            arguments.set(argument, expression.cast(settings, definition, variables));
        }

        statement = true;
        after = definition.defType;

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }

    @Override
    protected void load(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final StringBuilder signature = new StringBuilder();

        signature.append('(');
        // first parameter is the receiver, we never know its type: always Object
        signature.append(definition.defType.type.getDescriptor());

        // TODO: remove our explicit conversions and feed more type information for return value,
        // it can avoid some unnecessary boxing etc.
        for (final AExpression argument : arguments) {
            signature.append(argument.actual.type.getDescriptor());
            argument.write(settings, definition, adapter);
        }

        signature.append(')');
        // return value
        signature.append(definition.defType.type.getDescriptor());

        adapter.visitInvokeDynamicInsn(name, signature.toString(), DEF_BOOTSTRAP_HANDLE, new Object[] { DynamicCallSite.METHOD_CALL });
    }

    @Override
    protected void store(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }
}

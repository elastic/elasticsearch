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
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

public class LCall extends ALink {
    protected final String name;
    protected final List<AExpression> arguments;

    protected Method method = null;

    public LCall(final String location, final String name, final List<AExpression> arguments) {
        super(location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (before.sort == Definition.Sort.ARRAY) {
            throw new IllegalArgumentException(error("Illegal call [" + name + "] on array type."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a call [" + name + "]."));
        }

        final Struct struct = before.struct;
        method = statik ? struct.functions.get(name) : struct.methods.get(name);

        if (method != null) {
            final Definition.Type[] types = new Definition.Type[method.arguments.size()];
            method.arguments.toArray(types);

            if (method.arguments.size() != arguments.size()) {
                throw new IllegalArgumentException(error("When calling [" + name + "] on type [" + struct.name + "]" +
                    " expected [" + method.arguments.size() + "] arguments, but found [" + arguments.size() + "]."));
            }

            for (int argument = 0; argument < arguments.size(); ++argument) {
                final AExpression expression = arguments.get(argument);

                expression.expected = types[argument];
                expression.analyze(settings, definition, variables);
                arguments.set(argument, expression.cast(settings, definition, variables));
            }

            statement = true;
            after = method.rtn;

            return this;
        } else if (before.sort == Definition.Sort.DEF) {
            final ALink link = new LDefCall(location, name, arguments);
            link.copy(this);

            return link.analyze(settings, definition, variables);
        }

        throw new IllegalArgumentException(error("Unknown call [" + name + "] on type [" + struct.name + "]."));
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }

    @Override
    protected void load(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        for (final AExpression argument : arguments) {
            argument.write(settings, definition, adapter);
        }

        if (java.lang.reflect.Modifier.isStatic(method.reflect.getModifiers())) {
            adapter.invokeStatic(method.owner.type, method.method);
        } else if (java.lang.reflect.Modifier.isInterface(method.owner.clazz.getModifiers())) {
            adapter.invokeInterface(method.owner.type, method.method);
        } else {
            adapter.invokeVirtual(method.owner.type, method.method);
        }

        if (!method.rtn.clazz.equals(method.handle.type().returnType())) {
            adapter.checkCast(method.rtn.type);
        }
    }

    @Override
    protected void store(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }
}

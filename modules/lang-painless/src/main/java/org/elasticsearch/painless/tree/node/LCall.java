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
import org.elasticsearch.painless.tree.utility.Variables;

import java.util.List;

public class LCall extends Link {
    protected final String name;
    protected final List<Expression> arguments;

    public LCall(final String location, final String name, final List<Expression> arguments) {
        super(location);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (before.sort == Definition.Sort.ARRAY) {
            throw new IllegalArgumentException(error("Illegal call [" + name + "] on array type."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a call [" + name + "]."));
        }

        final Struct struct = before.struct;
        final Method method = statik ? struct.functions.get(name) : struct.methods.get(name);

        if (method != null) {
            final Definition.Type[] types = new Definition.Type[method.arguments.size()];
            method.arguments.toArray(types);

            if (method.arguments.size() != arguments.size()) {
                throw new IllegalArgumentException(error("When calling [" + name + "] on type [" + struct.name + "]" +
                    " expected [" + method.arguments.size() + "] arguments, but found [" + arguments.size() + "]."));
            }

            for (int argument = 0; argument < arguments.size(); ++argument) {
                final Expression expression = arguments.get(argument);

                expression.expected = types[argument];
                expression.analyze(settings, definition, variables);
                arguments.set(argument, expression.cast(definition));
            }

            after = method.rtn;
            target = new TCall(location, method, arguments);
        } else if (before.sort == Definition.Sort.DEF) {
            for (int argument = 0; argument < arguments.size(); ++argument) {
                final Expression expression = arguments.get(argument);

                expression.expected = definition.objectType;
                expression.analyze(settings, definition, variables);
                arguments.set(argument, expression.cast(definition));
            }

            after = definition.defType;
            target = new TDefCall(location, name, arguments);
        } else {
            throw new IllegalArgumentException(error("Unknown call [" + name + "] on type [" + struct.name + "]."));
        }

        statik = false;
        statement = true;
    }
}

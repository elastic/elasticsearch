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

import org.elasticsearch.painless.Definition.MethodKey;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;

/**
 * Represents a method call or deferes to a def call.
 */
public final class LCallInvoke extends ALink {

    final String name;
    final List<AExpression> arguments;

    Method method = null;

    public LCallInvoke(Location location, String name, List<AExpression> arguments) {
        super(location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(Locals locals) {
        if (before == null) {
            throw createError(new IllegalArgumentException("Illegal call [" + name + "] made without target."));
        } else if (before.sort == Sort.ARRAY) {
            throw createError(new IllegalArgumentException("Illegal call [" + name + "] on array type."));
        } else if (store) {
            throw createError(new IllegalArgumentException("Cannot assign a value to a call [" + name + "]."));
        }

        MethodKey methodKey = new MethodKey(name, arguments.size());
        Struct struct = before.struct;
        method = statik ? struct.staticMethods.get(methodKey) : struct.methods.get(methodKey);

        if (method != null) {
            for (int argument = 0; argument < arguments.size(); ++argument) {
                AExpression expression = arguments.get(argument);

                expression.expected = method.arguments.get(argument);
                expression.internal = true;
                expression.analyze(locals);
                arguments.set(argument, expression.cast(locals));
            }

            statement = true;
            after = method.rtn;

            return this;
        } else if (before.sort == Sort.DEF) {
            ALink link = new LDefCall(location, name, arguments);
            link.copy(this);

            return link.analyze(locals);
        }

        throw createError(new IllegalArgumentException(
            "Unknown call [" + name + "] with [" + arguments.size() + "] arguments on type [" + struct.name + "]."));
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(location);

        for (AExpression argument : arguments) {
            argument.write(writer);
        }

        if (java.lang.reflect.Modifier.isStatic(method.modifiers)) {
            writer.invokeStatic(method.owner.type, method.method);
        } else if (java.lang.reflect.Modifier.isInterface(method.owner.clazz.getModifiers())) {
            writer.invokeInterface(method.owner.type, method.method);
        } else {
            writer.invokeVirtual(method.owner.type, method.method);
        }
    }

    @Override
    void store(MethodWriter writer) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}

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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;

/**
 * Represents a method call or deferes to a def call.
 */
public final class LCall extends ALink {

    final String name;
    final List<AExpression> arguments;

    Method method = null;

    public LCall(int line, int offset, String location, String name, List<AExpression> arguments) {
        super(line, offset, location, -1);

        this.name = name;
        this.arguments = arguments;
    }

    @Override
    ALink analyze(Variables variables) {
        if (before == null) {
            throw new IllegalArgumentException(error("Illegal call [" + name + "] made without target."));
        } else if (before.sort == Sort.ARRAY) {
            throw new IllegalArgumentException(error("Illegal call [" + name + "] on array type."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a call [" + name + "]."));
        }

        Definition.MethodKey methodKey = new Definition.MethodKey(name, arguments.size());
        Struct struct = before.struct;
        method = statik ? struct.staticMethods.get(methodKey) : struct.methods.get(methodKey);

        if (method != null) {
            for (int argument = 0; argument < arguments.size(); ++argument) {
                AExpression expression = arguments.get(argument);

                expression.expected = method.arguments.get(argument);
                expression.internal = true;
                expression.analyze(variables);
                arguments.set(argument, expression.cast(variables));
            }

            statement = true;
            after = method.rtn;

            return this;
        } else if (before.sort == Sort.DEF) {
            ALink link = new LDefCall(line, offset, location, name, arguments);
            link.copy(this);

            return link.analyze(variables);
        }

        throw new IllegalArgumentException(error("Unknown call [" + name + "] with [" + arguments.size() +
                                                 "] arguments on type [" + struct.name + "]."));
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        for (AExpression argument : arguments) {
            argument.write(writer);
        }

        if (java.lang.reflect.Modifier.isStatic(method.reflect.getModifiers())) {
            writer.invokeStatic(method.owner.type, method.method);
        } else if (java.lang.reflect.Modifier.isInterface(method.owner.clazz.getModifiers())) {
            writer.invokeInterface(method.owner.type, method.method);
        } else {
            writer.invokeVirtual(method.owner.type, method.method);
        }

        if (!method.rtn.clazz.equals(method.handle.type().returnType())) {
            writer.checkCast(method.rtn.type);
        }
    }

    @Override
    void store(MethodWriter writer) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}

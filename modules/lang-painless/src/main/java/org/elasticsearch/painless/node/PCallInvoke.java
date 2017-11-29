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
import org.elasticsearch.painless.Definition.MethodKey;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a method call and defers to a child subnode.
 */
public final class PCallInvoke extends AExpression {

    private final String name;
    private final boolean nullSafe;
    private final List<AExpression> arguments;

    private AExpression sub = null;

    public PCallInvoke(Location location, AExpression prefix, String name, boolean nullSafe, List<AExpression> arguments) {
        super(location, prefix);

        this.name = Objects.requireNonNull(name);
        this.nullSafe = nullSafe;
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void extractVariables(Set<String> variables) {
        prefix.extractVariables(variables);

        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        prefix.analyze(locals);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(locals);

        if (prefix.actual.dimensions > 0) {
            throw createError(new IllegalArgumentException("Illegal call [" + name + "] on array type."));
        }

        Struct struct = prefix.actual.struct;

        if (prefix.actual.clazz.isPrimitive()) {
            struct = locals.getDefinition().getBoxedType(prefix.actual).struct;
        }

        MethodKey methodKey = new MethodKey(name, arguments.size());
        Method method = prefix instanceof EStatic ? struct.staticMethods.get(methodKey) : struct.methods.get(methodKey);

        if (method != null) {
            sub = new PSubCallInvoke(location, method, prefix.actual, arguments);
        } else if (prefix.actual.dynamic) {
            sub = new PSubDefCall(location, name, arguments);
        } else {
            throw createError(new IllegalArgumentException(
                "Unknown call [" + name + "] with [" + arguments.size() + "] arguments on type [" + struct.name + "]."));
        }

        if (nullSafe) {
            sub = new PSubNullSafeCallInvoke(location, sub);
        }

        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(locals);
        actual = sub.actual;

        statement = true;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        prefix.write(writer, globals);
        sub.write(writer, globals);
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, name);
    }
}

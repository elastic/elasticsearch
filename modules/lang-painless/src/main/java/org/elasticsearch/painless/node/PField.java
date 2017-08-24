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
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a field load/store and defers to a child subnode.
 */
public final class PField extends AStoreable {

    private final boolean nullSafe;
    private final String value;

    private AStoreable sub = null;

    public PField(Location location, AExpression prefix, boolean nullSafe, String value) {
        super(location, prefix);

        this.nullSafe = nullSafe;
        this.value = Objects.requireNonNull(value);
    }

    @Override
    void extractVariables(Set<String> variables) {
        prefix.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        prefix.analyze(locals);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(locals);

        if (prefix.actual.dimensions > 0) {
            sub = new PSubArrayLength(location, prefix.actual.name, value);
        } else if (prefix.actual.dynamic) {
            sub = new PSubDefField(location, value);
        } else {
            Struct struct = prefix.actual.struct;
            Field field = prefix instanceof EStatic ? struct.staticMembers.get(value) : struct.members.get(value);

            if (field != null) {
                sub = new PSubField(location, field);
            } else {
                Method getter = struct.methods.get(
                    new Definition.MethodKey("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0));

                if (getter == null) {
                    getter = struct.methods.get(
                        new Definition.MethodKey("is" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0));
                }

                Method setter = struct.methods.get(
                    new Definition.MethodKey("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 1));

                if (getter != null || setter != null) {
                    sub = new PSubShortcut(location, value, prefix.actual.name, getter, setter);
                } else {
                    EConstant index = new EConstant(location, value);
                    index.analyze(locals);

                    if (Map.class.isAssignableFrom(prefix.actual.clazz)) {
                        sub = new PSubMapShortcut(location, struct, index);
                    }

                    if (List.class.isAssignableFrom(prefix.actual.clazz)) {
                        sub = new PSubListShortcut(location, struct, index);
                    }
                }
            }
        }

        if (sub == null) {
            throw createError(new IllegalArgumentException("Unknown field [" + value + "] for type [" + prefix.actual.name + "]."));
        }

        if (nullSafe) {
            sub = new PSubNullSafeField(location, sub);
        }

        sub.write = write;
        sub.read = read;
        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(locals);
        actual = sub.actual;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        prefix.write(writer, globals);
        sub.write(writer, globals);
    }

    @Override
    boolean isDefOptimized() {
        return sub.isDefOptimized();
    }

    @Override
    void updateActual(Type actual) {
        sub.updateActual(actual);
        this.actual = actual;
    }

    @Override
    int accessElementCount() {
        return sub.accessElementCount();
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        prefix.write(writer, globals);
        sub.setup(writer, globals);
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        sub.load(writer, globals);
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        sub.store(writer, globals);
    }

    @Override
    public String toString() {
        if (nullSafe) {
            return singleLineToString("nullSafe", prefix, value);
        }
        return singleLineToString(prefix, value);
    }
}

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

import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Set;

/**
 * Represents a field load/store shortcut.  (Internal only.)
 */
final class PSubShortcut extends AStoreable {

    private final String value;
    private final String type;
    private final Method getter;
    private final Method setter;

    PSubShortcut(Location location, String value, String type, Method getter, Method setter) {
        super(location);

        this.value = value;
        this.type = type;
        this.getter = getter;
        this.setter = setter;
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        if (getter != null && (getter.rtn.clazz == void.class || !getter.arguments.isEmpty())) {
            throw createError(new IllegalArgumentException(
                "Illegal get shortcut on field [" + value + "] for type [" + type + "]."));
        }

        if (setter != null && (setter.rtn.clazz != void.class || setter.arguments.size() != 1)) {
            throw createError(new IllegalArgumentException(
                "Illegal set shortcut on field [" + value + "] for type [" + type + "]."));
        }

        if (getter != null && setter != null && setter.arguments.get(0) != getter.rtn) {
            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
        }

        if ((getter != null || setter != null) && (!read || getter != null) && (!write || setter != null)) {
            actual = setter != null ? setter.arguments.get(0) : getter.rtn;
        } else {
            throw createError(new IllegalArgumentException("Illegal shortcut on field [" + value + "] for type [" + type + "]."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        getter.write(writer);

        if (!getter.rtn.clazz.equals(getter.handle.type().returnType())) {
            writer.checkCast(getter.rtn.type);
        }
    }

    @Override
    int accessElementCount() {
        return 1;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Type actual) {
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        getter.write(writer);

        if (!getter.rtn.clazz.equals(getter.handle.type().returnType())) {
            writer.checkCast(getter.rtn.type);
        }
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        setter.write(writer);

        writer.writePop(setter.rtn.type.getSize());
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, value);
    }
}

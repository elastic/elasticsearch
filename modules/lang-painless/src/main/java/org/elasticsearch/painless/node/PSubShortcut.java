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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessMethod;

import java.util.Set;

/**
 * Represents a field load/store shortcut.  (Internal only.)
 */
final class PSubShortcut extends AStoreable {

    private final String value;
    private final String type;
    private final PainlessMethod getter;
    private final PainlessMethod setter;

    PSubShortcut(Location location, String value, String type, PainlessMethod getter, PainlessMethod setter) {
        super(location);

        this.value = value;
        this.type = type;
        this.getter = getter;
        this.setter = setter;
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        throw createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        if (getter != null && (getter.returnType == void.class || !getter.typeParameters.isEmpty())) {
            throw createError(new IllegalArgumentException(
                "Illegal get shortcut on field [" + value + "] for type [" + type + "]."));
        }

        if (setter != null && (setter.returnType != void.class || setter.typeParameters.size() != 1)) {
            throw createError(new IllegalArgumentException(
                "Illegal set shortcut on field [" + value + "] for type [" + type + "]."));
        }

        if (getter != null && setter != null && setter.typeParameters.get(0) != getter.returnType) {
            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
        }

        if ((getter != null || setter != null) && (!read || getter != null) && (!write || setter != null)) {
            actual = setter != null ? setter.typeParameters.get(0) : getter.returnType;
        } else {
            throw createError(new IllegalArgumentException("Illegal shortcut on field [" + value + "] for type [" + type + "]."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.invokeMethodCall(getter);

        if (!getter.returnType.equals(getter.javaMethod.getReturnType())) {
            writer.checkCast(MethodWriter.getType(getter.returnType));
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
    void updateActual(Class<?> actual) {
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.invokeMethodCall(getter);

        if (getter.returnType != getter.javaMethod.getReturnType()) {
            writer.checkCast(MethodWriter.getType(getter.returnType));
        }
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.invokeMethodCall(setter);

        writer.writePop(MethodWriter.getType(setter.returnType).getSize());
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, value);
    }
}

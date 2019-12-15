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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.def;

import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a field load/store or shortcut on a def type.  (Internal only.)
 */
final class PSubDefField extends AStoreable {

    private final String value;

    PSubDefField(Location location, String value) {
        super(location);

        this.value = Objects.requireNonNull(value);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
        actual = expected == null || expected == ZonedDateTime.class || explicit ? def.class : expected;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(MethodWriter.getType(actual), org.objectweb.asm.Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    int accessElementCount() {
        return 1;
    }

    @Override
    boolean isDefOptimized() {
        return true;
    }

    @Override
    void updateActual(Class<?> actual) {
        this.actual = actual;
    }

    @Override
    void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(MethodWriter.getType(actual), org.objectweb.asm.Type.getType(Object.class));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.LOAD);
    }

    @Override
    void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        org.objectweb.asm.Type methodType = org.objectweb.asm.Type.getMethodType(
            org.objectweb.asm.Type.getType(void.class), org.objectweb.asm.Type.getType(Object.class), MethodWriter.getType(actual));
        methodWriter.invokeDefCall(value, methodType, DefBootstrap.STORE);
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, value);
    }
}

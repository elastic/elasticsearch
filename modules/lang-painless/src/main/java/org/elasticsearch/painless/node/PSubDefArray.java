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

import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents an array load/store or shortcut on a def type.  (Internal only.)
 */
final class PSubDefArray extends AStoreable {

    private AExpression index;

    PSubDefArray(Location location, AExpression index) {
        super(location);

        this.index = Objects.requireNonNull(index);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        index.analyze(locals);
        index.expected = index.actual;
        index = index.cast(locals);

        actual = expected == null || explicit ? Definition.DEF_TYPE : expected;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        index.write(writer, globals);

        writer.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(actual.type, Definition.DEF_TYPE.type, index.actual.type);
        writer.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    int accessElementCount() {
        return 2;
    }

    @Override
    boolean isDefOptimized() {
        return true;
    }

    @Override
    void updateActual(Type actual) {
        this.actual = actual;
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        index.write(writer, globals);
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(actual.type, Definition.DEF_TYPE.type, index.actual.type);
        writer.invokeDefCall("arrayLoad", methodType, DefBootstrap.ARRAY_LOAD);
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        org.objectweb.asm.Type methodType =
            org.objectweb.asm.Type.getMethodType(Definition.VOID_TYPE.type, Definition.DEF_TYPE.type, index.actual.type, actual.type);
        writer.invokeDefCall("arrayStore", methodType, DefBootstrap.ARRAY_STORE);
    }
}

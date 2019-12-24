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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;

import java.util.Objects;
import java.util.Set;

/**
 * Represents an array load/store.
 */
final class PSubBrace extends AStoreable {

    private final Class<?> clazz;
    private AExpression index;

    PSubBrace(Location location, Class<?> clazz, AExpression index) {
        super(location);

        this.clazz = Objects.requireNonNull(clazz);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        index.expected = int.class;
        index.analyze(scriptRoot, locals);
        index = index.cast(scriptRoot, locals);

        actual = clazz.getComponentType();
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        setup(classWriter, methodWriter, globals);
        load(classWriter, methodWriter, globals);
    }

    @Override
    int accessElementCount() {
        return 2;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Class<?> actual) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        index.write(classWriter, methodWriter, globals);
        writeIndexFlip(methodWriter, MethodWriter::arrayLength);
    }

    @Override
    void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        methodWriter.arrayLoad(MethodWriter.getType(actual));
    }

    @Override
    void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        methodWriter.arrayStore(MethodWriter.getType(actual));
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}

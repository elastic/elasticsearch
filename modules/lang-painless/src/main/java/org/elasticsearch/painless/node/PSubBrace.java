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
    void storeSettings(CompilerSettings settings) {
        throw createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    void analyze(Locals locals) {
        index.expected = int.class;
        index.analyze(locals);
        index = index.cast(locals);

        actual = clazz.getComponentType();
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        setup(writer, globals);
        load(writer, globals);
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
    void setup(MethodWriter writer, Globals globals) {
        index.write(writer, globals);
        writeIndexFlip(writer, MethodWriter::arrayLength);
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);
        writer.arrayLoad(MethodWriter.getType(actual));
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);
        writer.arrayStore(MethodWriter.getType(actual));
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}

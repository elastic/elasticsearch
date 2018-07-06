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

import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessMethodKey;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a map load/store shortcut. (Internal only.)
 */
final class PSubMapShortcut extends AStoreable {

    private final PainlessClass struct;
    private AExpression index;

    private PainlessMethod getter;
    private PainlessMethod setter;

    PSubMapShortcut(Location location, PainlessClass struct, AExpression index) {
        super(location);

        this.struct = Objects.requireNonNull(struct);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        getter = struct.methods.get(new PainlessMethodKey("get", 1));
        setter = struct.methods.get(new PainlessMethodKey("put", 2));

        if (getter != null && (getter.rtn == void.class || getter.arguments.size() != 1)) {
            throw createError(new IllegalArgumentException("Illegal map get shortcut for type [" + struct.name + "]."));
        }

        if (setter != null && setter.arguments.size() != 2) {
            throw createError(new IllegalArgumentException("Illegal map set shortcut for type [" + struct.name + "]."));
        }

        if (getter != null && setter != null &&
            (!getter.arguments.get(0).equals(setter.arguments.get(0)) || !getter.rtn.equals(setter.arguments.get(1)))) {
            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
        }

        if ((read || write) && (!read || getter != null) && (!write || setter != null)) {
            index.expected = setter != null ? setter.arguments.get(0) : getter.arguments.get(0);
            index.analyze(locals);
            index = index.cast(locals);

            actual = setter != null ? setter.arguments.get(1) : getter.rtn;
        } else {
            throw createError(new IllegalArgumentException("Illegal map shortcut for type [" + struct.name + "]."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        index.write(writer, globals);

        writer.writeDebugInfo(location);

        getter.write(writer);

        if (getter.rtn != getter.handle.type().returnType()) {
            writer.checkCast(MethodWriter.getType(getter.rtn));
        }
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
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        index.write(writer, globals);
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        getter.write(writer);

        if (getter.rtn != getter.handle.type().returnType()) {
            writer.checkCast(MethodWriter.getType(getter.rtn));
        }
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        setter.write(writer);

        writer.writePop(MethodWriter.getType(setter.rtn).getSize());
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}

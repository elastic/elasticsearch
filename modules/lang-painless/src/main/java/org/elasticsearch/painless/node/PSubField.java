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

import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.lang.reflect.Modifier;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a field load/store.
 */
final class PSubField extends AStoreable {

    private final Field field;

    PSubField(Location location, Field field) {
        super(location);

        this.field = Objects.requireNonNull(field);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
         if (write && Modifier.isFinal(field.modifiers)) {
             throw createError(new IllegalArgumentException(
                 "Cannot write to read-only field [" + field.name + "] for type [" + field.type.name + "]."));
         }

        actual = field.type;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (java.lang.reflect.Modifier.isStatic(field.modifiers)) {
            writer.getStatic(field.owner.type, field.javaName, field.type.type);
        } else {
            writer.getField(field.owner.type, field.javaName, field.type.type);
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

        if (java.lang.reflect.Modifier.isStatic(field.modifiers)) {
            writer.getStatic(field.owner.type, field.javaName, field.type.type);
        } else {
            writer.getField(field.owner.type, field.javaName, field.type.type);
        }
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (java.lang.reflect.Modifier.isStatic(field.modifiers)) {
            writer.putStatic(field.owner.type, field.javaName, field.type.type);
        } else {
            writer.putField(field.owner.type, field.javaName, field.type.type);
        }
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, field.name);
    }
}

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
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;
import java.util.Map;

/**
 * Represents a field load/store or defers to a possible shortcuts.
 */
public final class LField extends ALink {

    final String value;

    Field field;

    public LField(final int line, final String location, final String value) {
        super(line, location, 1);

        this.value = value;
    }

    @Override
    ALink analyze(CompilerSettings settings, Definition definition, Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            return new LArrayLength(line, location, value).copy(this).analyze(settings, definition, variables);
        } else if (sort == Sort.DEF) {
            return new LDefField(line, location, value).copy(this).analyze(settings, definition, variables);
        }

        final Struct struct = before.struct;
        field = statik ? struct.statics.get(value) : struct.members.get(value);

        if (field != null) {
            if (store && java.lang.reflect.Modifier.isFinal(field.reflect.getModifiers())) {
                throw new IllegalArgumentException(error(
                    "Cannot write to read-only field [" + value + "] for type [" + struct.name + "]."));
            }

            after = field.type;

            return this;
        } else {
            final boolean shortcut =
                struct.methods.containsKey("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1)) ||
                struct.methods.containsKey("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));

            if (shortcut) {
                return new LShortcut(line, location, value).copy(this).analyze(settings, definition, variables);
            } else {
                final EConstant index = new EConstant(line, location, value);
                index.analyze(settings, definition, variables);

                if (Map.class.isAssignableFrom(before.clazz)) {
                    return new LMapShortcut(line, location, index).copy(this).analyze(settings, definition, variables);
                }
                
                if (List.class.isAssignableFrom(before.clazz)) {
                    return new LListShortcut(line, location, index).copy(this).analyze(settings, definition, variables);
                }
            }
        }

        throw new IllegalArgumentException(error("Unknown field [" + value + "] for type [" + struct.name + "]."));
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }

    @Override
    void load(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (java.lang.reflect.Modifier.isStatic(field.reflect.getModifiers())) {
            adapter.getStatic(field.owner.type, field.reflect.getName(), field.type.type);

            if (!field.generic.clazz.equals(field.type.clazz)) {
                adapter.checkCast(field.generic.type);
            }
        } else {
            adapter.getField(field.owner.type, field.reflect.getName(), field.type.type);

            if (!field.generic.clazz.equals(field.type.clazz)) {
                adapter.checkCast(field.generic.type);
            }
        }
    }

    @Override
    void store(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (java.lang.reflect.Modifier.isStatic(field.reflect.getModifiers())) {
            adapter.putStatic(field.owner.type, field.reflect.getName(), field.type.type);
        } else {
            adapter.putField(field.owner.type, field.reflect.getName(), field.type.type);
        }
    }
}

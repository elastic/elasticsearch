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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Map;

/**
 * Represents a field load/store or defers to a possible shortcuts.
 */
public final class LField extends ALink {

    final String value;

    Field field;

    public LField(int line, int offset, String location, String value) {
        super(line, offset, location, 1);

        this.value = value;
    }

    @Override
    ALink analyze(Variables variables) {
        if (before == null) {
            throw new IllegalArgumentException(error("Illegal field [" + value + "] access made without target."));
        }

        Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            return new LArrayLength(line, offset, location, value).copy(this).analyze(variables);
        } else if (sort == Sort.DEF) {
            return new LDefField(line, offset, location, value).copy(this).analyze(variables);
        }

        Struct struct = before.struct;
        field = statik ? struct.staticMembers.get(value) : struct.members.get(value);

        if (field != null) {
            if (store && java.lang.reflect.Modifier.isFinal(field.modifiers)) {
                throw new IllegalArgumentException(error(
                    "Cannot write to read-only field [" + value + "] for type [" + struct.name + "]."));
            }

            after = field.type;

            return this;
        } else {
            boolean shortcut =
                struct.methods.containsKey(new Definition.MethodKey("get" +
                    Character.toUpperCase(value.charAt(0)) + value.substring(1), 0)) ||
                struct.methods.containsKey(new Definition.MethodKey("is" +
                    Character.toUpperCase(value.charAt(0)) + value.substring(1), 0)) ||
                struct.methods.containsKey(new Definition.MethodKey("set" +
                    Character.toUpperCase(value.charAt(0)) + value.substring(1), 1));

            if (shortcut) {
                return new LShortcut(line, offset, location, value).copy(this).analyze(variables);
            } else {
                EConstant index = new EConstant(line, offset, location, value);
                index.analyze(variables);

                if (Map.class.isAssignableFrom(before.clazz)) {
                    return new LMapShortcut(line, offset, location, index).copy(this).analyze(variables);
                }

                if (List.class.isAssignableFrom(before.clazz)) {
                    return new LListShortcut(line, offset, location, index).copy(this).analyze(variables);
                }
            }
        }

        throw new IllegalArgumentException(error("Unknown field [" + value + "] for type [" + struct.name + "]."));
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(offset);
        if (java.lang.reflect.Modifier.isStatic(field.modifiers)) {
            writer.getStatic(field.owner.type, field.javaName, field.type.type);
        } else {
            writer.getField(field.owner.type, field.javaName, field.type.type);
        }
    }

    @Override
    void store(MethodWriter writer) {
        writer.writeDebugInfo(offset);
        if (java.lang.reflect.Modifier.isStatic(field.modifiers)) {
            writer.putStatic(field.owner.type, field.javaName, field.type.type);
        } else {
            writer.putField(field.owner.type, field.javaName, field.type.type);
        }
    }
}

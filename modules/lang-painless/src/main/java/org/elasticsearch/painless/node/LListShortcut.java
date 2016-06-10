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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a list load/store shortcut.  (Internal only.)
 */
final class LListShortcut extends ALink {

    AExpression index;
    Method getter;
    Method setter;

    LListShortcut(Location location, AExpression index) {
        super(location, 2);

        this.index = index;
    }

    @Override
    ALink analyze(Locals locals) {
        getter = before.struct.methods.get(new Definition.MethodKey("get", 1));
        setter = before.struct.methods.get(new Definition.MethodKey("set", 2));

        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
            getter.arguments.get(0).sort != Sort.INT)) {
            throw createError(new IllegalArgumentException("Illegal list get shortcut for type [" + before.name + "]."));
        }

        if (setter != null && (setter.arguments.size() != 2 || setter.arguments.get(0).sort != Sort.INT)) {
            throw createError(new IllegalArgumentException("Illegal list set shortcut for type [" + before.name + "]."));
        }

        if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
            || !getter.rtn.equals(setter.arguments.get(1)))) {
            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
        }

        if ((load || store) && (!load || getter != null) && (!store || setter != null)) {
            index.expected = Definition.INT_TYPE;
            index.analyze(locals);
            index = index.cast(locals);

            after = setter != null ? setter.arguments.get(1) : getter.rtn;
        } else {
            throw createError(new IllegalArgumentException("Illegal list shortcut for type [" + before.name + "]."));
        }

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        index.write(writer);
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(location);

        if (java.lang.reflect.Modifier.isInterface(getter.owner.clazz.getModifiers())) {
            writer.invokeInterface(getter.owner.type, getter.method);
        } else {
            writer.invokeVirtual(getter.owner.type, getter.method);
        }

        if (!getter.rtn.clazz.equals(getter.handle.type().returnType())) {
            writer.checkCast(getter.rtn.type);
        }
    }

    @Override
    void store(MethodWriter writer) {
        writer.writeDebugInfo(location);

        if (java.lang.reflect.Modifier.isInterface(setter.owner.clazz.getModifiers())) {
            writer.invokeInterface(setter.owner.type, setter.method);
        } else {
            writer.invokeVirtual(setter.owner.type, setter.method);
        }

        writer.writePop(setter.rtn.sort.size);
    }
}

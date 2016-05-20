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
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a list load/store shortcut.  (Internal only.)
 */
final class LListShortcut extends ALink {

    AExpression index;
    Method getter;
    Method setter;

    LListShortcut(final int line, final String location, final AExpression index) {
        super(line, location, 2);

        this.index = index;
    }

    @Override
    ALink analyze(final CompilerSettings settings, final Variables variables) {
        getter = before.struct.methods.get(new Definition.MethodKey("get", 1));
        setter = before.struct.methods.get(new Definition.MethodKey("set", 2));

        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
            getter.arguments.get(0).sort != Sort.INT)) {
            throw new IllegalArgumentException(error("Illegal list get shortcut for type [" + before.name + "]."));
        }

        if (setter != null && (setter.arguments.size() != 2 || setter.arguments.get(0).sort != Sort.INT)) {
            throw new IllegalArgumentException(error("Illegal list set shortcut for type [" + before.name + "]."));
        }

        if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
            || !getter.rtn.equals(setter.arguments.get(1)))) {
            throw new IllegalArgumentException(error("Shortcut argument types must match."));
        }

        if ((load || store) && (!load || getter != null) && (!store || setter != null)) {
            index.expected = Definition.intType;
            index.analyze(settings, variables);
            index = index.cast(settings, variables);

            after = setter != null ? setter.arguments.get(1) : getter.rtn;
        } else {
            throw new IllegalArgumentException(error("Illegal list shortcut for type [" + before.name + "]."));
        }

        return this;
    }

    @Override
    void write(final CompilerSettings settings, final MethodWriter adapter) {
        index.write(settings, adapter);
    }

    @Override
    void load(final CompilerSettings settings, final MethodWriter adapter) {
        if (java.lang.reflect.Modifier.isInterface(getter.owner.clazz.getModifiers())) {
            adapter.invokeInterface(getter.owner.type, getter.method);
        } else {
            adapter.invokeVirtual(getter.owner.type, getter.method);
        }

        if (!getter.rtn.clazz.equals(getter.handle.type().returnType())) {
            adapter.checkCast(getter.rtn.type);
        }
    }

    @Override
    void store(final CompilerSettings settings, final MethodWriter adapter) {
        if (java.lang.reflect.Modifier.isInterface(setter.owner.clazz.getModifiers())) {
            adapter.invokeInterface(setter.owner.type, setter.method);
        } else {
            adapter.invokeVirtual(setter.owner.type, setter.method);
        }

        adapter.writePop(setter.rtn.sort.size);
    }
}

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
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.WriterUtility;
import org.objectweb.asm.commons.GeneratorAdapter;

public class LShortcut extends ALink {
    protected final String value;

    protected Method getter = null;
    protected Method setter = null;

    protected LShortcut(final String location, final String value) {
        super(location, 1);

        this.value = value;
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        final Struct struct = before.struct;

        getter = struct.methods.get("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
        setter = struct.methods.get("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));

        if (getter != null && (getter.rtn.sort == Sort.VOID || !getter.arguments.isEmpty())) {
            throw new IllegalArgumentException(error(
                "Illegal get shortcut on field [" + value + "] for type [" + struct.name + "]."));
        }

        if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 1)) {
            throw new IllegalArgumentException(error(
                "Illegal set shortcut on field [" + value + "] for type [" + struct.name + "]."));
        }

        if (getter != null && setter != null && setter.arguments.get(0) != getter.rtn) {
            throw new IllegalArgumentException(error("Shortcut argument types must match."));
        }

        if ((getter != null || setter != null) && (!load || getter != null) && (!store || setter != null)) {
            after = setter != null ? setter.arguments.get(0) : getter.rtn;
        } else {
            throw new IllegalArgumentException(error("Illegal shortcut on field [" + value + "] for type [" + struct.name + "]."));
        }

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        // Do nothing.
    }

    @Override
    protected void load(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
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
    protected void store(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (java.lang.reflect.Modifier.isInterface(setter.owner.clazz.getModifiers())) {
            adapter.invokeInterface(setter.owner.type, setter.method);
        } else {
            adapter.invokeVirtual(setter.owner.type, setter.method);
        }

        WriterUtility.writePop(adapter, setter.rtn.sort.size);
    }
}

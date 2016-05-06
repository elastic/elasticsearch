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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

public class LMapShortcut extends ALink {
    protected AExpression index;
    protected Method getter;
    protected Method setter;

    public LMapShortcut(final String location, final AExpression index) {
        super(location);

        this.index = index;
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        getter = before.struct.methods.get("get");
        setter = before.struct.methods.get("put");

        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1)) {
            throw new IllegalArgumentException(error("Illegal map get shortcut for type [" + before.name + "]."));
        }

        if (setter != null && setter.arguments.size() != 2) {
            throw new IllegalArgumentException(error("Illegal map set shortcut for type [" + before.name + "]."));
        }

        if (getter != null && setter != null &&
            (!getter.arguments.get(0).equals(setter.arguments.get(0)) || !getter.rtn.equals(setter.arguments.get(1)))) {
            throw new IllegalArgumentException(error("Shortcut argument types must match."));
        }

        if ((load || store) && (!load || getter != null) && (!store || setter != null)) {
            index.expected = setter != null ? setter.arguments.get(0) : getter.arguments.get(0);
            index.analyze(settings, definition, variables);
            index = index.cast(settings, definition, variables);

            after = setter != null ? setter.arguments.get(1) : getter.rtn;
        } else {
            throw new IllegalArgumentException(error("Illegal map shortcut for type [" + before.name + "]."));
        }

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (begincat) {
            Shared.writeNewStrings(adapter);
        }

        index.write(settings, definition, adapter);

        if (store) {
            if (endcat) {
                adapter.dup2X1();
                adapter.arrayLoad(before.type);
                Shared.writeAppendStrings(adapter, after.sort);

                expression.write(settings, definition, adapter);

                if (!(expression instanceof EBinary) ||
                    ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                    Shared.writeAppendStrings(adapter, expression.expected.sort);
                }

                Shared.writeToStrings(adapter);
                Shared.writeCast(adapter, back);

                if (load) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                adapter.arrayStore(before.type);
            } else if (operation != null) {
                adapter.dup2();
                load(adapter);

                if (load && post) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                Shared.writeCast(adapter, there);
                expression.write(settings, definition, adapter);
                Shared.writeBinaryInstruction(settings, definition, adapter, location, there.to, operation);

                if (settings.getNumericOverflow() || !expression.typesafe ||
                    !Shared.writeExactInstruction(definition, adapter, expression.actual.sort, after.sort)) {
                    Shared.writeCast(adapter, back);
                }

                store(adapter);
            } else {
                if (load) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                store(adapter);
            }
        } else {
            load(adapter);
        }
    }

    protected void load(final GeneratorAdapter adapter) {
        if (java.lang.reflect.Modifier.isInterface(getter.owner.clazz.getModifiers())) {
            adapter.invokeInterface(getter.owner.type, getter.method);
        } else {
            adapter.invokeVirtual(getter.owner.type, getter.method);
        }

        if (!getter.rtn.clazz.equals(getter.handle.type().returnType())) {
            adapter.checkCast(getter.rtn.type);
        }
    }

    protected void store(final GeneratorAdapter adapter) {
        if (java.lang.reflect.Modifier.isInterface(setter.owner.clazz.getModifiers())) {
            adapter.invokeInterface(setter.owner.type, setter.method);
        } else {
            adapter.invokeVirtual(setter.owner.type, setter.method);
        }

        Shared.writePop(adapter, setter.rtn.sort.size);
    }
}

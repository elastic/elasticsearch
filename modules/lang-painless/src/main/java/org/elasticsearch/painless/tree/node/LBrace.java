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
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;
import java.util.Map;

public class LBrace extends Link {
    protected Expression expression;

    public LBrace(final String location, final Expression expression) {
        super(location);

        this.expression = expression;
    }

    @Override
    protected Link analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            expression.expected = definition.intType;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(definition);

            after = definition.getType(before.struct, before.dimensions - 1);

            return this;
        } else if (sort == Sort.DEF) {
            expression.expected = definition.objectType;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(definition);

            after = definition.defType;
            target = new TDefArray(location, expression);
        } else {
            boolean map = false;
            boolean list = false;

            try {
                before.clazz.asSubclass(Map.class);
                map = true;
            } catch (final ClassCastException exception) {
                // Do nothing.
            }

            try {
                before.clazz.asSubclass(List.class);
                list = true;
            } catch (final ClassCastException exception) {
                // Do nothing.
            }

            final boolean load = !last || parent.read;
            final boolean store = last && parent.expression != null;

            if (map) {
                final Method getter = before.struct.methods.get("get");
                final Method setter = before.struct.methods.get("put");

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
                    expression.expected = setter != null ? setter.arguments.get(0) : getter.arguments.get(0);
                    expression.analyze(settings, definition, variables);
                    expression = expression.cast(definition);

                    after = setter != null ? setter.arguments.get(1) : getter.rtn;
                    target = new TMapShortcut(location, getter, setter, expression);
                } else {
                    throw new IllegalArgumentException(error("Illegal map shortcut for type [" + before.name + "]."));
                }
            } else if (list) {
                final Method getter = before.struct.methods.get("get");
                final Method setter = before.struct.methods.get("set");

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
                    expression.expected = definition.intType;
                    expression.analyze(settings, definition, variables);
                    expression = expression.cast(definition);

                    after = setter != null ? setter.arguments.get(1) : getter.rtn;
                    target = new TListShortcut(location, getter, setter, expression);
                } else {
                    throw new IllegalArgumentException(error("Illegal list shortcut for type [" + before.name + "]."));
                }
            } else {
                throw new IllegalArgumentException(error("Illegal array access on type [" + before.name + "]."));
            }
        }
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (first && parent.cat) {
            Shared.writeNewStrings(adapter);
        }

        expression.write(adapter);

        if (last && parent.expression != null) {
            if (parent.cat) {
                adapter.dup2X1();
                adapter.arrayLoad(before.type);
                Shared.writeAppendStrings(adapter, after.sort);
                parent.expression.write(adapter);
                Shared.writeToStrings(adapter);
                Shared.writeCast(adapter, parent.back);

                if (parent.read) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                adapter.arrayStore(before.type);
            } else if (parent.operation != null) {
                adapter.dup2();
                adapter.arrayLoad(before.type);

                if (parent.read && parent.post) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                Shared.writeCast(adapter, parent.there);
                parent.expression.write(adapter);

                if (settings.getNumericOverflow() || !parent.expression.typesafe || !parent.operation.exact ||
                    !Shared.writeExactInstruction(definition, adapter, parent.expression.actual.sort, after.sort)) {
                    Shared.writeCast(adapter, parent.back);
                }


            } else {

            }
        } else {
            adapter.arrayLoad(before.type);
        }
    }
}

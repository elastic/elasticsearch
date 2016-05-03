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
import org.elasticsearch.painless.tree.utility.Variables;

import java.util.List;
import java.util.Map;

public class LBrace extends Link {
    protected Expression expression;

    public LBrace(final String location, final Expression expression) {
        super(location);

        this.expression = expression;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            expression.expected = definition.intType;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(definition);

            after = definition.getType(before.struct, before.type.getDimensions() - 1);
            target = new TArray(location, before, expression);
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
}

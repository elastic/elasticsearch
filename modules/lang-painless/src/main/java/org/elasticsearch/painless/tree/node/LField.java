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
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.tree.analyzer.Variables;

import java.util.List;
import java.util.Map;

public class LField extends ALink {
    protected final String value;

    protected Field field;

    public LField(final String location, final String value) {
        super(location);

        this.value = value;
    }

    @Override
    protected ALink analyze(CompilerSettings settings, Definition definition, Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            return new LArrayLength(location, value).copy(this).analyze(settings, definition, variables);
        } else if (sort == Sort.DEF) {
            return new LDefField(location, value).copy(this).analyze(settings, definition, variables);
        } else {
            final Struct struct = before.struct;
            final Field field = statik ? struct.statics.get(value) : struct.members.get(value);

            if (field != null) {
                if (store && java.lang.reflect.Modifier.isFinal(field.reflect.getModifiers())) {
                    throw new IllegalArgumentException(error(
                        "Cannot write to read-only field [" + value + "] for type [" + struct.name + "]."));
                }

                after = field.type;
                target = new TField(location, field);
            } else {
                Method getter = struct.methods.get("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Method setter = struct.methods.get("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));

                if (getter != null && (getter.rtn.sort == Sort.VOID || !getter.arguments.isEmpty())) {
                    throw new IllegalArgumentException(error(
                        "Illegal get shortcut on field [" + value + "] for type [" + struct.name + "]."));
                }

                if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(error(
                        "Illegal set shortcut on field [" + value + "] for type [" + struct.name + "]."));
                }

                if ((getter != null || setter != null) && (load || store) &&
                    (!load || getter != null) && (!store || setter != null)) {
                    after = load ? getter.rtn : setter.rtn;
                    target = new TShortcut(location, getter, setter);
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
                        getter = struct.methods.get("get");
                        setter = struct.methods.get("put");

                        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                            getter.arguments.get(0).sort != Sort.STRING)) {
                            throw new IllegalArgumentException(error(
                                "Illegal map get shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (setter != null && (setter.arguments.size() != 2 ||
                            setter.arguments.get(0).sort != Sort.STRING)) {
                            throw new IllegalArgumentException(error(
                                "Illegal map set shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                            throw new IllegalArgumentException(error("Shortcut argument types must match."));
                        }

                        if ((load || store) && (!load || getter != null) && (!store || setter != null)) {
                            final EConstant econstant = new EConstant(location, value);

                            after = load ? getter.rtn : setter.rtn;
                            target = new LMapShortcut(location, getter, setter, econstant);
                        } else {
                            throw new IllegalArgumentException(error(
                                "Illegal map shortcut [" + value + "] for type [" + struct.name + "]."));
                        }
                    } else if (list) {
                        getter = struct.methods.get("get");
                        setter = struct.methods.get("set");

                        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                            getter.arguments.get(0).sort != Sort.INT)) {
                            throw new IllegalArgumentException(error(
                                "Illegal list get shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 2 ||
                            setter.arguments.get(0).sort != Sort.INT)) {
                            throw new IllegalArgumentException(error(
                                "Illegal list set shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                            throw new IllegalArgumentException(error("Shortcut argument types must match."));
                        }

                        if ((load || store) && (!load || getter != null) && (!store || setter != null)) {
                            try {
                                final EConstant econstant = new EConstant(location, Integer.parseInt(value));

                                after = load ? getter.rtn : setter.rtn;
                                target = new LMapShortcut(location, getter, setter, econstant);
                            } catch (final NumberFormatException exception) {
                                throw new IllegalArgumentException(error("Illegal list shortcut value [" + value + "]."));
                            }

                        } else {
                            throw new IllegalArgumentException(error(
                                "Illegal map shortcut [" + value + "] for type [" + struct.name + "]."));
                        }
                    } else {
                        throw new IllegalArgumentException(error("Unknown field [" + value + "] for type [" + struct.name + "]."));
                    }
                }
            }
        }

        statik = false;
    }
}

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

package org.elasticsearch.painless.tree.walker.analyzer;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Constructor;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Struct;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.utility.Operation;
import org.elasticsearch.painless.tree.utility.Variables;
import org.elasticsearch.painless.tree.utility.Variables.Variable;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.painless.tree.utility.Operation.ADD;
import static org.elasticsearch.painless.tree.utility.Operation.AND;
import static org.elasticsearch.painless.tree.utility.Operation.DIV;
import static org.elasticsearch.painless.tree.utility.Operation.LSH;
import static org.elasticsearch.painless.tree.utility.Operation.MUL;
import static org.elasticsearch.painless.tree.utility.Operation.OR;
import static org.elasticsearch.painless.tree.utility.Operation.REM;
import static org.elasticsearch.painless.tree.utility.Operation.RSH;
import static org.elasticsearch.painless.tree.utility.Operation.SUB;
import static org.elasticsearch.painless.tree.utility.Operation.USH;
import static org.elasticsearch.painless.tree.utility.Operation.XOR;
import static org.elasticsearch.painless.tree.node.Type.AALENGTH;
import static org.elasticsearch.painless.tree.node.Type.ACONSTANT;
import static org.elasticsearch.painless.tree.node.Type.ADEFBRACE;
import static org.elasticsearch.painless.tree.node.Type.ADEFCALL;
import static org.elasticsearch.painless.tree.node.Type.ADEFFIELD;
import static org.elasticsearch.painless.tree.node.Type.ALISTSHORTCUT;
import static org.elasticsearch.painless.tree.node.Type.AMAPSHORTCUT;
import static org.elasticsearch.painless.tree.node.Type.ASHORTCUT;

class AnalyzerExternal {
    private final Definition definition;
    private final Variables variables;
    private final Analyzer analyzer;
    private final AnalyzerCaster caster;
    private final AnalyzerPromoter promoter;

    AnalyzerExternal(final Definition definition, final Variables variables,
                     final Analyzer analyzer, final AnalyzerCaster caster, final AnalyzerPromoter promoter) {
        this.definition = definition;
        this.variables = variables;
        this.analyzer = analyzer;
        this.caster = caster;
        this.promoter = promoter;
    }

    void visitExternal(final Node external, final MetadataExpression externalme) {
        if (external.children.isEmpty()) {
            throw new IllegalStateException(external.error("Illegal tree structure."));
        }

        MetadataExternal childme = new MetadataExternal();
        MetadataExternal previousme = null;
        int index = 0;

        while (true) {
            final Node child = external.children.get(index);

            if (previousme != null) {
                childme.before = previousme.after;
                childme.statik = previousme.statik;
            }

            if (index == external.children.size() - 1) {
                childme.load = externalme.read;
            }

            final Node node = analyzer.visit(child, childme);

            if (node == null) {
                external.children.remove(index);
            } else {
                if (node != child) {
                    external.children.set(index, node);
                }

                ++index;
            }

            if (index == external.children.size()) {
                break;
            }

            previousme = childme;
            childme = new MetadataExternal();
        }

        externalme.statement = childme.statement;
        externalme.constant = childme.constant;
        externalme.actual = childme.after;
        externalme.typesafe = childme.after.sort != Sort.DEF;
    }

    void visitPostinc(final Node postinc, final MetadataExpression postincme) {
        MetadataExternal childme = new MetadataExternal();
        MetadataExternal previousme = null;
        int index = 0;

        while (true) {
            final Node child = postinc.children.get(index);

            if (previousme != null) {
                childme.before = previousme.after;
                childme.statik = previousme.statik;
            }

            if (index == postinc.children.size() - 1) {
                childme.load = postincme.read;
                childme.store = true;
            }

            final Node node = analyzer.visit(child, childme);

            if (node == null) {
                postinc.children.remove(index);
            } else {
                if (node != child) {
                    postinc.children.set(index, node);
                }

                ++index;
            }

            if (index == postinc.children.size()) {
                break;
            }

            previousme = childme;
            childme = new MetadataExternal();
        }

        final Type promote = promoter.promoteNumeric(childme.after, true, true);

        if (promote == null) {
            throw new IllegalArgumentException(postinc.error("Cannot apply post increment to type [" + childme.after + "]."));
        }

        final Node there = caster.markCast(postinc, childme.after, promote, false);
        final Node back = caster.markCast(postinc, promote, childme.after, true);

        final Node store = postinc.children.get(postinc.children.size() - 1);
        store.children.add(there);
        store.children.add(back);

        postincme.statement = true;
        postincme.actual = postincme.read ? childme.after : definition.voidType;
        postincme.typesafe = childme.after.sort != Sort.DEF;
    }

    void visitPreinc(final Node preinc, final MetadataExpression preincme) {
        MetadataExternal childme = new MetadataExternal();
        MetadataExternal previousme = null;
        int index = 0;

        while (true) {
            final Node child = preinc.children.get(index);

            if (previousme != null) {
                childme.before = previousme.after;
                childme.statik = previousme.statik;
            }

            if (index == preinc.children.size() - 1) {
                childme.load = preincme.read;
                childme.store = true;
            }

            final Node node = analyzer.visit(child, childme);

            if (node == null) {
                preinc.children.remove(index);
            } else {
                if (node != child) {
                    preinc.children.set(index, node);
                }

                ++index;
            }

            if (index == preinc.children.size()) {
                break;
            }

            previousme = childme;
            childme = new MetadataExternal();
        }

        final Type promote = promoter.promoteNumeric(childme.after, true, true);

        if (promote == null) {
            throw new IllegalArgumentException(preinc.error("Cannot apply pre increment to type [" + childme.after + "]."));
        }

        final Node there = caster.markCast(preinc, childme.after, promote, false);
        final Node back = caster.markCast(preinc, promote, childme.after, true);

        final Node store = preinc.children.get(preinc.children.size() - 1);
        store.children.add(there);
        store.children.add(back);

        preincme.statement = true;
        preincme.actual = preincme.read ? childme.after : definition.voidType;
        preincme.typesafe = childme.after.sort != Sort.DEF;
    }

    void visitAssignment(final Node assign, final MetadataExpression assignme) {
        MetadataExternal childme = new MetadataExternal();
        MetadataExternal previousme = null;
        int index = 0;

        while (true) {
            final Node child = assign.children.get(index);

            if (previousme != null) {
                childme.before = previousme.after;
                childme.statik = previousme.statik;
            }

            if (index == assign.children.size() - 2) {
                childme.load = assignme.read;
                childme.store = true;
            }

            final Node node = analyzer.visit(child, childme);

            if (node == null) {
                assign.children.remove(index);
            } else {
                if (node != child) {
                    assign.children.set(index, node);
                }

                ++index;
            }

            if (index == assign.children.size() - 1) {
                break;
            }

            previousme = childme;
            childme = new MetadataExternal();
        }

        final Node expression = assign.children.get(assign.children.size() - 1);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = childme.after;
        analyzer.visit(expression, expressionme);
        assign.children.set(assign.children.size() - 1, caster.markCast(expression, expressionme));

        assignme.statement = true;
        assignme.actual = assignme.read ? childme.after : definition.voidType;
        assignme.typesafe = childme.after.sort != Sort.DEF;
    }

    void visitCompound(final Node compound, final MetadataExpression compoundme) {
        MetadataExternal childme = new MetadataExternal();
        MetadataExternal previousme = null;
        int index = 0;

        while (true) {
            final Node child = compound.children.get(index);

            if (previousme != null) {
                childme.before = previousme.after;
                childme.statik = previousme.statik;
            }

            if (index == compound.children.size() - 2) {
                childme.load = compoundme.read;
                childme.store = true;
            }

            final Node node = analyzer.visit(child, childme);

            if (node == null) {
                compound.children.remove(index);
            } else {
                if (node != child) {
                    compound.children.set(index, node);
                }

                ++index;
            }

            if (index == compound.children.size() - 1) {
                break;
            }

            previousme = childme;
            childme = new MetadataExternal();
        }

        final Node expression = compound.children.get(compound.children.size() - 1);
        final MetadataExpression expressionme = new MetadataExpression();

        analyzer.visit(expression, expressionme);

        final Operation operation = (Operation)compound.data.get("operation");
        final Type promote;

        if (operation == MUL) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, true, true);
        } else if (operation == DIV) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, true, true);
        } else if (operation == REM) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, true, true);
        } else if (operation == ADD) {
            promote = promoter.promoteAdd(childme.after, expressionme.actual);
        } else if (operation == SUB) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, true, true);
        } else if (operation == LSH) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, false, true);
        } else if (operation == USH) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, false, true);
        } else if (operation == RSH) {
            promote = promoter.promoteNumeric(childme.after, expressionme.actual, false, true);
        } else if (operation == AND) {
            promote = promoter.promoteXor(childme.after, expressionme.actual);
        } else if (operation == XOR) {
            promote = promoter.promoteXor(childme.after, expressionme.actual);
        } else if (operation == OR) {
            promote = promoter.promoteXor(childme.after, expressionme.actual);
        } else {
            throw new IllegalStateException(compound.error("Illegal tree structure."));
        }

        if (promote == null) {
            throw new IllegalArgumentException(compound.error(
                "Cannot apply compound assignment to types [" + childme.after + "] and [" + expressionme.actual + "]."));
        }

        final Node there = caster.markCast(compound, childme.after, promote, false);
        final Node back = caster.markCast(compound, promote, childme.after, true);

        final Node store = compound.children.get(compound.children.size() - 2);
        store.children.add(there);
        store.children.add(back);

        expressionme.expected = promote.sort == Sort.STRING ? expressionme.actual : promote;

        compoundme.statement = true;
        compoundme.actual = compoundme.read ? childme.after : definition.voidType;
        compoundme.typesafe = childme.after.sort != Sort.DEF;
    }

    Node visitCast(final Node cast, final MetadataExternal castme) {
        if (castme.before == null) {
            throw new IllegalStateException(cast.error("Illegal tree structure."));
        } else if (castme.store) {
            throw new IllegalArgumentException(cast.error("Cannot assign a value to a cast."));
        }

        final String typestr = (String)cast.data.get("type");
        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(cast.error("Not a type [" + typestr + "]."));
        }

        castme.after = type;

        return caster.markCast(cast, castme.before, castme.after, true);
    }

    Node visitBrace(final Node brace, final MetadataExternal braceme) {
        if (braceme.before == null) {
            throw new IllegalStateException(brace.error("Illegal tree structure."));
        }

        final Node rtn;

        final Type before = braceme.before;
        final Sort sort = before.sort;

        final Node expression = brace.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        if (sort == Sort.ARRAY) {
            rtn = brace;

            expressionme.expected = definition.intType;
            analyzer.visit(expression, expressionme);
            brace.children.set(0, caster.markCast(expression, expressionme));

            braceme.after = definition.getType(before.struct, before.type.getDimensions() - 1);
        } else if (sort == Sort.DEF) {
            rtn = new Node(brace.location, ADEFBRACE);

            expressionme.expected = definition.objectType;
            analyzer.visit(expression, expressionme);
            rtn.children.add(caster.markCast(expression, expressionme));

            braceme.after = definition.defType;
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
                    throw new IllegalArgumentException(brace.error("Illegal map get shortcut for type [" + before.name + "]."));
                }

                if (setter != null && setter.arguments.size() != 2) {
                    throw new IllegalArgumentException(brace.error("Illegal map set shortcut for type [" + before.name + "]."));
                }

                if (getter != null && setter != null &&
                    (!getter.arguments.get(0).equals(setter.arguments.get(0)) || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(brace.error("Shortcut argument types must match."));
                }

                if ((braceme.load || braceme.store) && (!braceme.load || getter != null) && (!braceme.store || setter != null)) {
                    rtn = new Node(brace.location, AMAPSHORTCUT);
                    rtn.data.put("getter", getter);
                    rtn.data.put("setter", setter);

                    expressionme.expected = setter != null ? setter.arguments.get(0) : getter.arguments.get(0);
                    analyzer.visit(expression, expressionme);
                    rtn.children.add(caster.markCast(expression, expressionme));

                    braceme.after = setter != null ? setter.arguments.get(1) : getter.rtn;
                } else {
                    throw new IllegalArgumentException(brace.error("Illegal map shortcut for type [" + before.name + "]."));
                }
            } else if (list) {
                final Method getter = before.struct.methods.get("get");
                final Method setter = before.struct.methods.get("set");

                if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                    getter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(brace.error("Illegal list get shortcut for type [" + before.name + "]."));
                }

                if (setter != null && (setter.arguments.size() != 2 || setter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(brace.error("Illegal list set shortcut for type [" + before.name + "]."));
                }

                if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
                    || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(brace.error("Shortcut argument types must match."));
                }

                if ((braceme.load || braceme.store) && (!braceme.load || getter != null) && (!braceme.store || setter != null)) {
                    rtn = new Node(brace.location, ALISTSHORTCUT);
                    rtn.data.put("getter", getter);
                    rtn.data.put("setter", setter);

                    expressionme.expected = definition.intType;
                    analyzer.visit(expression, expressionme);
                    rtn.children.add(caster.markCast(expression, expressionme));

                    braceme.after = setter != null ? setter.arguments.get(1) : getter.rtn;
                } else {
                    throw new IllegalArgumentException(brace.error("Illegal list shortcut for type [" + before.name + "]."));
                }
            } else {
                throw new IllegalArgumentException(brace.error("Illegal array access on type [" + before.name + "]."));
            }
        }

        return rtn;
    }

    Node visitCall(final Node call, final MetadataExternal callme) {
        final Node rtn;

        final String name = (String)call.data.get("symbol");

        if (callme.before == null) {
            throw new IllegalStateException(call.error("Illegal tree structure."));
        } else if (callme.before.sort == Sort.ARRAY) {
            throw new IllegalArgumentException(call.error("Illegal call [" + name + "] on array type."));
        } else if (callme.store) {
            throw new IllegalArgumentException(call.error("Cannot assign a value to a call [" + name + "]."));
        }

        final Struct struct = callme.before.struct;
        final Method method = callme.statik ? struct.functions.get(name) : struct.methods.get(name);

        if (method != null) {
            rtn = call;
            rtn.data.put("method", method);

            final Type[] types = new Type[method.arguments.size()];
            method.arguments.toArray(types);

            if (method.arguments.size() != call.children.size()) {
                throw new IllegalArgumentException(call.error("When calling [" + name + "] on type [" + struct.name + "]" +
                    " expected [" + method.arguments.size() + "] arguments, but found [" + call.children.size() + "]."));
            }

            int argument = 0;

            for (final Node child : call.children) {
                final MetadataExpression childme = new MetadataExpression();

                childme.expected = types[argument];
                analyzer.visit(child, childme);
                final Node cast = caster.markCast(child, childme);

                if (cast != child) {
                    call.children.set(argument, cast);
                }

                ++argument;
            }

            callme.after = method.rtn;
        } else if (callme.before.sort == Sort.DEF) {
            rtn = new Node(call.location, ADEFCALL);

            final boolean[] typesafe = new boolean[call.children.size()];
            int argument = 0;

            for (final Node child : call.children) {
                final MetadataExpression childme = new MetadataExpression();

                childme.expected = definition.objectType;
                analyzer.visit(child, childme);
                final Node cast = caster.markCast(child, childme);
                rtn.children.add(cast);

                typesafe[argument] = childme.typesafe;

                ++argument;
            }

            rtn.data.put("name", name);
            rtn.data.put("typesafe", typesafe);

            callme.after = definition.defType;
        } else {
            throw new IllegalArgumentException(call.error("Unknown call [" + name + "] on type [" + struct.name + "]."));
        }

        callme.statik = false;
        callme.statement = true;

        return rtn;
    }

    Node visitVar(final Node var, final MetadataExternal varme) {
        if (varme.before != null) {
            throw new IllegalStateException(var.error("Illegal tree structure."));
        }

        final String name = (String)var.data.get("symbol");

        Type type = null;

        try {
            type = definition.getType(name);
        } catch (final IllegalArgumentException exception) {
            // Do nothing.
        }

        if (type != null) {
            varme.statik = true;
            varme.after = type;

            return null;
        } else {
            final Variable variable = variables.getVariable(var.location, name);
            var.data.put("variable", variable);

            varme.after = variable.type;

            return var;
        }
    }

    Node visitField(final Node node, final MetadataExternal nodeme) {
        if (nodeme.before == null) {
            throw new IllegalStateException(node.error("Illegal tree structure."));
        }

        final Node rtn;

        final String value = (String)node.data.get("symbol");
        final Type before = nodeme.before;
        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            if ("length".equals(value)) {
                rtn = new Node(node.location, AALENGTH);

                if (!nodeme.load) {
                    throw new IllegalArgumentException(node.error("Must read array field [length]."));
                } else if (nodeme.store) {
                    throw new IllegalArgumentException(node.error("Cannot write to read-only array field [length]."));
                }

                nodeme.after = definition.intType;
            } else {
                throw new IllegalArgumentException(node.error("Illegal field access [" + value + "]."));
            }
        } else if (sort == Sort.DEF) {
            rtn = new Node(node.location, ADEFFIELD);
            rtn.data.put("value", value);

            nodeme.after = definition.defType;
        } else {
            final Struct struct = before.struct;
            final Field field = nodeme.statik ? struct.statics.get(value) : struct.members.get(value);

            if (field != null) {
                rtn = node;

                if (nodeme.store && java.lang.reflect.Modifier.isFinal(field.reflect.getModifiers())) {
                    throw new IllegalArgumentException(node.error(
                        "Cannot write to read-only field [" + value + "] for type [" + struct.name + "]."));
                }

                rtn.data.put("field", field);

                nodeme.after = field.type;
            } else {
                Method getter = struct.methods.get("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Method setter = struct.methods.get("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));

                if (getter != null && (getter.rtn.sort == Sort.VOID || !getter.arguments.isEmpty())) {
                    throw new IllegalArgumentException(node.error(
                        "Illegal get shortcut on field [" + value + "] for type [" + struct.name + "]."));
                }

                if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(node.error(
                        "Illegal set shortcut on field [" + value + "] for type [" + struct.name + "]."));
                }

                if ((getter != null || setter != null) && (nodeme.load || nodeme.store) &&
                    (!nodeme.load || getter != null) && (!nodeme.store || setter != null)) {
                    rtn = new Node(node.location, ASHORTCUT);
                    rtn.data.put("getter", getter);
                    rtn.data.put("setter", setter);

                    nodeme.after = nodeme.load ? getter.rtn : setter.rtn;
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
                            throw new IllegalArgumentException(node.error(
                                "Illegal map get shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (setter != null && (setter.arguments.size() != 2 ||
                            setter.arguments.get(0).sort != Sort.STRING)) {
                            throw new IllegalArgumentException(node.error(
                                "Illegal map set shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                            throw new IllegalArgumentException(node.error("Shortcut argument types must match."));
                        }

                        if ((nodeme.load || nodeme.store) && (!nodeme.load || getter != null) && (!nodeme.store || setter != null)) {
                            rtn = new Node(node.location, AMAPSHORTCUT);
                            rtn.data.put("getter", getter);
                            rtn.data.put("setter", setter);

                            final Node constant = new Node(node.location, ACONSTANT);
                            constant.data.put("constant", value);

                            rtn.children.add(constant);

                            nodeme.after = nodeme.load ? getter.rtn : setter.rtn;
                        } else {
                            throw new IllegalArgumentException(node.error(
                                "Illegal map shortcut [" + value + "] for type [" + struct.name + "]."));
                        }
                    } else if (list) {
                        getter = struct.methods.get("get");
                        setter = struct.methods.get("set");

                        if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                            getter.arguments.get(0).sort != Sort.INT)) {
                            throw new IllegalArgumentException(node.error(
                                "Illegal list get shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 2 ||
                            setter.arguments.get(0).sort != Sort.INT)) {
                            throw new IllegalArgumentException(node.error(
                                "Illegal list set shortcut [" + value + "] for type [" + struct.name + "]."));
                        }

                        if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                            throw new IllegalArgumentException(node.error("Shortcut argument types must match."));
                        }

                        if ((nodeme.load || nodeme.store) && (!nodeme.load || getter != null) && (!nodeme.store || setter != null)) {
                            rtn = new Node(node.location, ALISTSHORTCUT);
                            rtn.data.put("getter", getter);
                            rtn.data.put("setter", setter);

                            try {
                                final Node constant = new Node(node.location, ACONSTANT);
                                constant.data.put("constant", Integer.parseInt(value));

                                rtn.children.add(constant);
                            } catch (final NumberFormatException exception) {
                                throw new IllegalArgumentException(node.error("Illegal list shortcut value [" + value + "]."));
                            }

                            nodeme.after = nodeme.load ? getter.rtn : setter.rtn;
                        } else {
                            throw new IllegalArgumentException(node.error(
                                "Illegal map shortcut [" + value + "] for type [" + struct.name + "]."));
                        }
                    } else {
                        throw new IllegalArgumentException(node.error("Unknown field [" + value + "] for type [" + struct.name + "]."));
                    }
                }
            }
        }

        nodeme.statik = false;

        return rtn;
    }

    Node visitNewobj(final Node newobj, final MetadataExternal newobjme) {
        if (newobjme.before != null) {
            throw new IllegalStateException(newobj.error("Illegal tree structure"));
        } else if (newobjme.store) {
            throw new IllegalArgumentException(newobj.error("Cannot assign a value to a new call."));
        }

        final String typestr = (String)newobj.data.get("symbol");
        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(newobj.error("Not a type [" + typestr + "]."));
        }

        final Struct struct = type.struct;
        final Constructor constructor = struct.constructors.get("new");

        if (constructor != null) {
            newobj.data.put("constructor", constructor);

            final Type[] types = new Type[constructor.arguments.size()];
            constructor.arguments.toArray(types);

            if (constructor.arguments.size() != newobj.children.size()) {
                throw new IllegalArgumentException(newobj.error("When calling constructor on type [" + struct.name + "]" +
                    " expected [" + constructor.arguments.size() + "] arguments, but found [" + newobj.children.size() + "]."));
            }

            int argument = 0;

            for (final Node child : newobj.children) {
                final MetadataExpression childme = new MetadataExpression();

                childme.expected = types[argument];
                analyzer.visit(child, childme);
                final Node cast = caster.markCast(child, childme);

                if (cast != child) {
                    newobj.children.set(argument, cast);
                }

                ++argument;
            }

            newobjme.after = type;
            newobjme.statement = true;
        } else {
            throw new IllegalArgumentException(newobj.error("Unknown new call on type [" + struct.name + "]."));
        }

        return newobj;
    }

    Node visitNewarray(final Node newarray, final MetadataExternal newarrayme) {
        if (newarrayme.before != null) {
            throw new IllegalStateException(newarray.error("Illegal tree structure."));
        } else if (newarrayme.store) {
            throw new IllegalArgumentException(newarray.error("Cannot assign a value to a new array."));
        } else if (!newarrayme.load) {
            throw new IllegalArgumentException(newarray.error("A newly created array must be assigned."));
        }

        final String typestr = (String)newarray.data.get("symbol");
        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(newarray.error("Not a type [" + typestr + "]."));
        }

        newarray.data.put("type", type);

        int argument = 0;

        for (final Node child : newarray.children) {
            final MetadataExpression childme = new MetadataExpression();

            childme.expected = definition.intType;
            analyzer.visit(child, childme);
            final Node cast = caster.markCast(child, childme);

            if (cast != child) {
                newarray.children.set(argument, cast);
            }

            ++argument;
        }

        newarrayme.after = definition.getType(type.struct, newarray.children.size());

        return newarray;
    }

    Node visitString(final Node string, final MetadataExternal stringme) {
        if (stringme.before != null) {
            throw new IllegalStateException("Illegal tree structure.");
        }

        final Object constant = string.data.get("string");

        if (stringme.store) {
            throw new IllegalArgumentException(string.error("Cannot write to read-only String constant [" + string + "]."));
        } else if (!stringme.load) {
            throw new IllegalArgumentException(string.error("Must read String constant [" + string + "]."));
        }

        stringme.after = definition.stringType;
        stringme.constant = constant;

        return string;
    }
}

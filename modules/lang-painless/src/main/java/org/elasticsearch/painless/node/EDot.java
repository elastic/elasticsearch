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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.DotNode;
import org.elasticsearch.painless.ir.DotSubArrayLengthNode;
import org.elasticsearch.painless.ir.DotSubDefNode;
import org.elasticsearch.painless.ir.DotSubNode;
import org.elasticsearch.painless.ir.DotSubShortcutNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.ListSubShortcutNode;
import org.elasticsearch.painless.ir.MapSubShortcutNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.lang.reflect.Modifier;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a field load/store and defers to a child subnode.
 */
public class EDot extends AExpression {

    protected final AExpression prefix;
    protected final boolean nullSafe;
    protected final String value;

    public EDot(Location location, AExpression prefix, boolean nullSafe, String value) {
        super(location);

        this.prefix = Objects.requireNonNull(prefix);
        this.nullSafe = nullSafe;
        this.value = Objects.requireNonNull(value);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.read == false && input.write == false) {
            throw createError(new IllegalArgumentException("not a statement: result of dot operator [.] not used"));
        }

        Output output = new Output();
        Output prefixOutput = prefix.analyze(classNode, scriptRoot, scope, new Input());

        if (prefixOutput.partialCanonicalTypeName != null) {
            if (output.isStaticType) {
                throw createError(new IllegalArgumentException("value required: " +
                        "instead found unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "]"));
            }

            String canonicalTypeName = prefixOutput.partialCanonicalTypeName + "." + value;
            Class<?> type = scriptRoot.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

            if (type == null) {
                output.partialCanonicalTypeName = canonicalTypeName;
            } else {
                if (input.write) {
                    throw createError(new IllegalArgumentException("invalid assignment: " +
                            "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "]"));
                }

                if (input.read == false) {
                    throw createError(new IllegalArgumentException(
                            "not a statement: static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] not used"));
                }

                output.actual = type;
                output.isStaticType = true;

                StaticNode staticNode = new StaticNode();

                staticNode.setLocation(location);
                staticNode.setExpressionType(output.actual);

                output.expressionNode = staticNode;
            }
        } else {
            Class<?> targetType = prefixOutput.actual;
            String targetCanonicalTypeName = PainlessLookupUtility.typeToCanonicalTypeName(targetType);

            ExpressionNode expressionNode = null;

            if (prefixOutput.actual.isArray()) {
                if (output.isStaticType) {
                    throw createError(new IllegalArgumentException("value required: " +
                            "instead found unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "]"));
                }

                if ("length".equals(value)) {
                    if (input.write) {
                        throw createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value write to read-only field [length] for an array."));
                    }

                    output.actual = int.class;
                } else {
                    throw createError(new IllegalArgumentException(
                            "Field [" + value + "] does not exist for type [" + targetCanonicalTypeName + "]."));
                }

                DotSubArrayLengthNode dotSubArrayLengthNode = new DotSubArrayLengthNode();
                dotSubArrayLengthNode.setLocation(location);
                dotSubArrayLengthNode.setExpressionType(output.actual);
                expressionNode = dotSubArrayLengthNode;
            } else if (prefixOutput.actual == def.class) {
                if (output.isStaticType) {
                    throw createError(new IllegalArgumentException("value required: " +
                            "instead found unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(output.actual) + "]"));
                }

                // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
                output.actual =
                        input.expected == null || input.expected == ZonedDateTime.class || input.explicit ? def.class : input.expected;
                output.isDefOptimized = true;

                DotSubDefNode dotSubDefNode = new DotSubDefNode();
                dotSubDefNode.setLocation(location);
                dotSubDefNode.setExpressionType(output.actual);
                dotSubDefNode.setValue(value);
                expressionNode = dotSubDefNode;
            } else {
                PainlessField field =
                        scriptRoot.getPainlessLookup().lookupPainlessField(prefixOutput.actual, prefixOutput.isStaticType, value);

                if (field == null) {
                    PainlessMethod getter;
                    PainlessMethod setter;

                    getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                            "get" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);

                    if (getter == null) {
                        getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                                "is" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);
                    }

                    setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                            "set" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);

                    if (getter != null || setter != null) {
                        if (getter != null && (getter.returnType == void.class || !getter.typeParameters.isEmpty())) {
                            throw createError(new IllegalArgumentException(
                                    "Illegal get shortcut on field [" + value + "] for type [" + targetCanonicalTypeName + "]."));
                        }

                        if (setter != null && (setter.returnType != void.class || setter.typeParameters.size() != 1)) {
                            throw createError(new IllegalArgumentException(
                                    "Illegal set shortcut on field [" + value + "] for type [" + targetCanonicalTypeName + "]."));
                        }

                        if (getter != null && setter != null && setter.typeParameters.get(0) != getter.returnType) {
                            throw createError(new IllegalArgumentException("Shortcut argument types must match."));
                        }

                        if ((input.read == false || getter != null) && (input.write == false || setter != null)) {
                            output.actual = setter != null ? setter.typeParameters.get(0) : getter.returnType;
                        } else {
                            throw createError(new IllegalArgumentException(
                                    "Illegal shortcut on field [" + value + "] for type [" + targetCanonicalTypeName + "]."));
                        }

                        DotSubShortcutNode dotSubShortcutNode = new DotSubShortcutNode();
                        dotSubShortcutNode.setLocation(location);
                        dotSubShortcutNode.setExpressionType(output.actual);
                        dotSubShortcutNode.setGetter(getter);
                        dotSubShortcutNode.setSetter(setter);
                        expressionNode = dotSubShortcutNode;
                    } else {
                        if (Map.class.isAssignableFrom(prefixOutput.actual)) {
                            getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetType, false, "get", 1);
                            setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetType, false, "put", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
                                throw createError(new IllegalArgumentException(
                                        "Illegal map get shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            if (setter != null && setter.typeParameters.size() != 2) {
                                throw createError(new IllegalArgumentException(
                                        "Illegal map set shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                                    !getter.returnType.equals(setter.typeParameters.get(1)))) {
                                throw createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }

                            if ((input.read == false || getter != null) && (input.write == false || setter != null)) {
                                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw createError(new IllegalArgumentException(
                                        "Illegal map shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            ConstantNode constantNode = new ConstantNode();
                            constantNode.setLocation(location);
                            constantNode.setExpressionType(String.class);
                            constantNode.setConstant(value);

                            MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
                            mapSubShortcutNode.setChildNode(constantNode);
                            mapSubShortcutNode.setLocation(location);
                            mapSubShortcutNode.setExpressionType(output.actual);
                            mapSubShortcutNode.setGetter(getter);
                            mapSubShortcutNode.setSetter(setter);
                            expressionNode = mapSubShortcutNode;
                        }

                        if (List.class.isAssignableFrom(prefixOutput.actual)) {
                            int index;

                            try {
                                index = Integer.parseInt(value);
                            } catch (NumberFormatException nfe) {
                                throw createError(new IllegalArgumentException("invalid list index [" + value + "]"));
                            }

                            getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetType, false, "get", 1);
                            setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(targetType, false, "set", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1 ||
                                    getter.typeParameters.get(0) != int.class)) {
                                throw createError(new IllegalArgumentException(
                                        "Illegal list get shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            if (setter != null && (setter.typeParameters.size() != 2 || setter.typeParameters.get(0) != int.class)) {
                                throw createError(new IllegalArgumentException(
                                        "Illegal list set shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0))
                                    || !getter.returnType.equals(setter.typeParameters.get(1)))) {
                                throw createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }

                            if ((input.read == false || getter != null) && (input.write == false || setter != null)) {
                                output.actual = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw createError(new IllegalArgumentException(
                                        "Illegal list shortcut for type [" + targetCanonicalTypeName + "]."));
                            }

                            ConstantNode constantNode = new ConstantNode();
                            constantNode.setLocation(location);
                            constantNode.setExpressionType(int.class);
                            constantNode.setConstant(index);

                            ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
                            listSubShortcutNode.setChildNode(constantNode);
                            listSubShortcutNode.setLocation(location);
                            listSubShortcutNode.setExpressionType(output.actual);
                            listSubShortcutNode.setGetter(getter);
                            listSubShortcutNode.setSetter(setter);
                            expressionNode = listSubShortcutNode;
                        }
                    }

                    if (expressionNode == null) {
                        throw createError(new IllegalArgumentException(
                                "field [" + typeToCanonicalTypeName(prefixOutput.actual) + ", " + value + "] not found"));
                    }
                } else {
                    if (input.write && Modifier.isFinal(field.javaField.getModifiers())) {
                        throw createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value to read-only field [" + field.javaField.getName() + "]"));
                    }

                    output.actual = field.typeParameter;

                    DotSubNode dotSubNode = new DotSubNode();
                    dotSubNode.setLocation(location);
                    dotSubNode.setExpressionType(output.actual);
                    dotSubNode.setField(field);
                    expressionNode = dotSubNode;
                }
            }

            if (nullSafe) {
                if (input.write) {
                    throw createError(new IllegalArgumentException(
                            "invalid assignment: cannot assign a value to a null safe operation [?.]"));
                }

                if (output.actual.isPrimitive()) {
                    throw new IllegalArgumentException("Result of null safe operator must be nullable");
                }

                NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
                nullSafeSubNode.setChildNode(expressionNode);
                nullSafeSubNode.setLocation(location);
                nullSafeSubNode.setExpressionType(output.actual);
                expressionNode = nullSafeSubNode;
            }

            DotNode dotNode = new DotNode();
            dotNode.setLeftNode(prefixOutput.expressionNode);
            dotNode.setRightNode(expressionNode);
            dotNode.setLocation(location);
            dotNode.setExpressionType(output.actual);
            output.expressionNode = dotNode;
        }

        return output;
    }
}

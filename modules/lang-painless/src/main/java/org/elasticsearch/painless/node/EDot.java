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
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.DefOptimized;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.ListShortcut;
import org.elasticsearch.painless.symbol.Decorations.MapShortcut;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.SetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.Shortcut;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessField;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.lang.reflect.Modifier;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a field load/store and defers to a child subnode.
 */
public class EDot extends AExpression {

    private final AExpression prefixNode;
    private final String index;
    private final boolean isNullSafe;

    public EDot(int identifier, Location location, AExpression prefixNode, String index, boolean isNullSafe) {
        super(identifier, location);

        this.prefixNode = Objects.requireNonNull(prefixNode);
        this.index = Objects.requireNonNull(index);
        this.isNullSafe = isNullSafe;
    }

    public AExpression getPrefixNode() {
        return prefixNode;
    }

    public String getIndex() {
        return index;
    }

    public boolean isNullSafe() {
        return isNullSafe;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitDot(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, EDot userDotNode, SemanticScope semanticScope) {

        boolean read = semanticScope.getCondition(userDotNode, Read.class);
        boolean write = semanticScope.getCondition(userDotNode, Write.class);

        if (read == false && write == false) {
            throw userDotNode.createError(new IllegalArgumentException("not a statement: result of dot operator [.] not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        String index = userDotNode.getIndex();

        AExpression userPrefixNode = userDotNode.getPrefixNode();
        semanticScope.setCondition(userPrefixNode, Read.class);
        visitor.visit(userPrefixNode, semanticScope);
        ValueType prefixValueType = semanticScope.getDecoration(userPrefixNode, ValueType.class);
        StaticType prefixStaticType = semanticScope.getDecoration(userPrefixNode, StaticType.class);

        if (prefixValueType != null && prefixStaticType != null) {
            throw userDotNode.createError(new IllegalStateException("cannot have both " +
                    "value [" + prefixValueType.getValueCanonicalTypeName() + "] " +
                    "and type [" + prefixStaticType.getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(userPrefixNode, PartialCanonicalTypeName.class)) {
            if (prefixValueType != null) {
                throw userDotNode.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                        "[" + prefixValueType.getValueCanonicalTypeName() + "]"));
            }

            if (prefixStaticType != null) {
                throw userDotNode.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                        "[" + prefixStaticType.getStaticType() + "]"));
            }

            String canonicalTypeName =
                    semanticScope.getDecoration(userPrefixNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "." + index;
            Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

            if (type == null) {
                semanticScope.putDecoration(userDotNode, new PartialCanonicalTypeName(canonicalTypeName));
            } else {
                if (write) {
                    throw userDotNode.createError(new IllegalArgumentException("invalid assignment: " +
                            "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "]"));
                }

                semanticScope.putDecoration(userDotNode, new StaticType(type));
            }
        } else {
            Class<?> valueType = null;

            if (prefixValueType != null && prefixValueType.getValueType().isArray()) {
                if ("length".equals(index)) {
                    if (write) {
                        throw userDotNode.createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value write to read-only field [length] for an array."));
                    }

                    valueType = int.class;
                } else {
                    throw userDotNode.createError(new IllegalArgumentException(
                            "Field [" + index + "] does not exist for type [" + prefixValueType.getValueCanonicalTypeName() + "]."));
                }
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                TargetType targetType = semanticScope.getDecoration(userDotNode, TargetType.class);
                // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
                valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                        semanticScope.getCondition(userDotNode, Explicit.class) ? def.class : targetType.getTargetType();
                semanticScope.setCondition(userDotNode, DefOptimized.class);
            } else {
                Class<?> prefixType;
                String prefixCanonicalTypeName;
                boolean isStatic;

                if (prefixValueType != null) {
                    prefixType = prefixValueType.getValueType();
                    prefixCanonicalTypeName = prefixValueType.getValueCanonicalTypeName();
                    isStatic = false;
                } else if (prefixStaticType != null) {
                    prefixType = prefixStaticType.getStaticType();
                    prefixCanonicalTypeName = prefixStaticType.getStaticCanonicalTypeName();
                    isStatic = true;
                } else {
                    throw userDotNode.createError(new IllegalStateException("value required: instead found no value"));
                }

                PainlessField field = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessField(prefixType, isStatic, index);

                if (field == null) {
                    PainlessMethod getter;
                    PainlessMethod setter;

                    getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                            "get" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);

                    if (getter == null) {
                        getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                                "is" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);
                    }

                    setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                            "set" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);

                    if (getter != null || setter != null) {
                        if (getter != null && (getter.returnType == void.class || !getter.typeParameters.isEmpty())) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal get shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (setter != null && (setter.returnType != void.class || setter.typeParameters.size() != 1)) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal set shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (getter != null && setter != null && setter.typeParameters.get(0) != getter.returnType) {
                            throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                        }

                        if ((read == false || getter != null) && (write == false || setter != null)) {
                            valueType = setter != null ? setter.typeParameters.get(0) : getter.returnType;
                        } else {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (getter != null) {
                            semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                        }

                        if (setter != null) {
                            semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                        }

                        semanticScope.setCondition(userDotNode, Shortcut.class);
                    } else if (isStatic == false) {
                        if (Map.class.isAssignableFrom(prefixValueType.getValueType())) {
                            getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "get", 1);
                            setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "put", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map get shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (setter != null && setter.typeParameters.size() != 2) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map set shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                                    getter.returnType.equals(setter.typeParameters.get(1)) == false)) {
                                throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }

                            if ((read == false || getter != null) && (write == false || setter != null)) {
                                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null) {
                                semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                            }

                            if (setter != null) {
                                semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                            }

                            semanticScope.setCondition(userDotNode, MapShortcut.class);
                        }

                        if (List.class.isAssignableFrom(prefixType)) {
                            try {
                                scriptScope.putDecoration(userDotNode, new StandardConstant(Integer.parseInt(index)));
                            } catch (NumberFormatException nfe) {
                                throw userDotNode.createError(new IllegalArgumentException("invalid list index [" + index + "]", nfe));
                            }

                            getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "get", 1);
                            setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "set", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1 ||
                                    getter.typeParameters.get(0) != int.class)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list get shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (setter != null && (setter.typeParameters.size() != 2 || setter.typeParameters.get(0) != int.class)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list set shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0))
                                    || !getter.returnType.equals(setter.typeParameters.get(1)))) {
                                throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }
                            
                            if ((read == false || getter != null) && (write == false || setter != null)) {
                                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null) {
                                semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                            }

                            if (setter != null) {
                                semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                            }

                            semanticScope.setCondition(userDotNode, ListShortcut.class);
                        }
                    }

                    if (valueType == null) {
                        if (prefixValueType != null) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "field [" + prefixValueType.getValueCanonicalTypeName() + ", " + index + "] not found"));
                        } else {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "field [" + prefixStaticType.getStaticCanonicalTypeName() + ", " + index + "] not found"));
                        }
                    }
                } else {
                    if (write && Modifier.isFinal(field.javaField.getModifiers())) {
                        throw userDotNode.createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value to read-only field [" + field.javaField.getName() + "]"));
                    }

                    semanticScope.putDecoration(userDotNode, new StandardPainlessField(field));
                    valueType = field.typeParameter;
                }
            }

            semanticScope.putDecoration(userDotNode, new ValueType(valueType));

            if (userDotNode.isNullSafe()) {
                if (write) {
                    throw userDotNode.createError(new IllegalArgumentException(
                            "invalid assignment: cannot assign a value to a null safe operation [?.]"));
                }

                if (valueType.isPrimitive()) {
                    throw new IllegalArgumentException("Result of null safe operator must be nullable");
                }
            }
        }
    }
}

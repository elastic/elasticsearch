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
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a method call and defers to a child subnode.
 */
public class ECall extends AExpression {

    private final AExpression prefixNode;
    private final String methodName;
    private final List<AExpression> argumentNodes;
    private final boolean isNullSafe;

    public ECall(int identifier, Location location,
            AExpression prefixNode, String methodName, List<AExpression> argumentNodes, boolean isNullSafe) {

        super(identifier, location);

        this.prefixNode = Objects.requireNonNull(prefixNode);
        this.methodName = Objects.requireNonNull(methodName);
        this.argumentNodes = Collections.unmodifiableList(Objects.requireNonNull(argumentNodes));
        this.isNullSafe = isNullSafe;
    }

    public AExpression getPrefixNode() {
        return prefixNode;
    }

    public String getMethodName() {
        return methodName;
    }

    public List<AExpression> getArgumentNodes() {
        return argumentNodes;
    }

    public boolean isNullSafe() {
        return isNullSafe;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitCall(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to method call [" + methodName + "/" + argumentNodes.size() + "]"));
        }

        semanticScope.setCondition(prefixNode, Read.class);
        prefixNode.analyze(semanticScope);
        ValueType prefixValueType = semanticScope.getDecoration(prefixNode, ValueType.class);
        StaticType prefixStaticType = semanticScope.getDecoration(prefixNode, StaticType.class);

        if (prefixValueType != null && prefixStaticType != null) {
            throw createError(new IllegalStateException("cannot have both " +
                    "value [" + prefixValueType.getValueCanonicalTypeName() + "] " +
                    "and type [" + prefixStaticType.getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(prefixNode, PartialCanonicalTypeName.class)) {
            throw createError(new IllegalArgumentException("cannot resolve symbol " +
                    "[" + semanticScope.getDecoration(prefixNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "]"));
        }

        Class<?> valueType;

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            for (AExpression argument : argumentNodes) {
                semanticScope.setCondition(argument, Read.class);
                semanticScope.setCondition(argument, Internal.class);
                analyze(argument, semanticScope);
                Class<?> argumentValueType = semanticScope.getDecoration(argument, ValueType.class).getValueType();

                if (argumentValueType == void.class) {
                    throw createError(new IllegalArgumentException(
                            "Argument(s) cannot be of [void] type when calling method [" + methodName + "]."));
                }
            }

            TargetType targetType = semanticScope.getDecoration(this, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(this, Explicit.class) ? def.class : targetType.getTargetType();
        } else {
            PainlessMethod method;

            if (prefixValueType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixValueType.getValueType(), false, methodName, argumentNodes.size());

                if (method == null) {
                    throw createError(new IllegalArgumentException("member method " +
                            "[" + prefixValueType.getValueCanonicalTypeName() + ", " + methodName + "/" + argumentNodes.size() + "] " +
                            "not found"));
                }
            } else if (prefixStaticType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixStaticType.getStaticType(), true, methodName, argumentNodes.size());

                if (method == null) {
                    throw createError(new IllegalArgumentException("static method " +
                            "[" + prefixStaticType.getStaticCanonicalTypeName() + ", " + methodName + "/" + argumentNodes.size() + "] " +
                            "not found"));
                }
            } else {
                throw createError(new IllegalStateException("value required: instead found no value"));
            }

            semanticScope.getScriptScope().markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            for (int argument = 0; argument < argumentNodes.size(); ++argument) {
                AExpression expression = argumentNodes.get(argument);

                semanticScope.setCondition(expression, Read.class);
                semanticScope.putDecoration(expression, new TargetType(method.typeParameters.get(argument)));
                semanticScope.setCondition(expression, Internal.class);
                analyze(expression, semanticScope);
                expression.cast(semanticScope);
            }

            semanticScope.putDecoration(this, new StandardPainlessMethod(method));
            valueType = method.returnType;
        }

        if (isNullSafe && valueType.isPrimitive()) {
            throw new IllegalArgumentException("Result of null safe operator must be nullable");
        }

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}

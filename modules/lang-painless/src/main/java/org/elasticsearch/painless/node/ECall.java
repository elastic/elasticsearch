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
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
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

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ECall userCallNode, SemanticScope semanticScope) {

        String methodName = userCallNode.getMethodName();
        List<AExpression> userArgumentNodes = userCallNode.getArgumentNodes();
        int userArgumentsSize = userArgumentNodes.size();

        if (semanticScope.getCondition(userCallNode, Write.class)) {
            throw userCallNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to method call [" + methodName + "/" + userArgumentsSize + "]"));
        }

        AExpression userPrefixNode = userCallNode.getPrefixNode();
        semanticScope.setCondition(userPrefixNode, Read.class);
        visitor.visit(userPrefixNode, semanticScope);
        ValueType prefixValueType = semanticScope.getDecoration(userPrefixNode, ValueType.class);
        StaticType prefixStaticType = semanticScope.getDecoration(userPrefixNode, StaticType.class);

        if (prefixValueType != null && prefixStaticType != null) {
            throw userCallNode.createError(new IllegalStateException("cannot have both " +
                    "value [" + prefixValueType.getValueCanonicalTypeName() + "] " +
                    "and type [" + prefixStaticType.getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(userPrefixNode, PartialCanonicalTypeName.class)) {
            throw userCallNode.createError(new IllegalArgumentException("cannot resolve symbol " +
                    "[" + semanticScope.getDecoration(userPrefixNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "]"));
        }

        Class<?> valueType;

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            for (AExpression userArgumentNode : userArgumentNodes) {
                semanticScope.setCondition(userArgumentNode, Read.class);
                semanticScope.setCondition(userArgumentNode, Internal.class);
                visitor.checkedVisit(userArgumentNode, semanticScope);
                Class<?> argumentValueType = semanticScope.getDecoration(userArgumentNode, ValueType.class).getValueType();

                if (argumentValueType == void.class) {
                    throw userCallNode.createError(new IllegalArgumentException(
                            "Argument(s) cannot be of [void] type when calling method [" + methodName + "]."));
                }
            }

            TargetType targetType = semanticScope.getDecoration(userCallNode, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(userCallNode, Explicit.class) ? def.class : targetType.getTargetType();
        } else {
            PainlessMethod method;

            if (prefixValueType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixValueType.getValueType(), false, methodName, userArgumentsSize);

                if (method == null) {
                    throw userCallNode.createError(new IllegalArgumentException("member method " +
                            "[" + prefixValueType.getValueCanonicalTypeName() + ", " + methodName + "/" + userArgumentsSize + "] " +
                            "not found"));
                }
            } else if (prefixStaticType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixStaticType.getStaticType(), true, methodName, userArgumentsSize);

                if (method == null) {
                    throw userCallNode.createError(new IllegalArgumentException("static method " +
                            "[" + prefixStaticType.getStaticCanonicalTypeName() + ", " + methodName + "/" + userArgumentsSize + "] " +
                            "not found"));
                }
            } else {
                throw userCallNode.createError(new IllegalStateException("value required: instead found no value"));
            }

            semanticScope.getScriptScope().markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            for (int argument = 0; argument < userArgumentsSize; ++argument) {
                AExpression userArgumentNode = userArgumentNodes.get(argument);

                semanticScope.setCondition(userArgumentNode, Read.class);
                semanticScope.putDecoration(userArgumentNode, new TargetType(method.typeParameters.get(argument)));
                semanticScope.setCondition(userArgumentNode, Internal.class);
                visitor.checkedVisit(userArgumentNode, semanticScope);
                visitor.decorateWithCast(userArgumentNode, semanticScope);
            }

            semanticScope.putDecoration(userCallNode, new StandardPainlessMethod(method));
            valueType = method.returnType;
        }

        if (userCallNode.isNullSafe() && valueType.isPrimitive()) {
            throw new IllegalArgumentException("Result of null safe operator must be nullable");
        }

        semanticScope.putDecoration(userCallNode, new ValueType(valueType));
    }
}

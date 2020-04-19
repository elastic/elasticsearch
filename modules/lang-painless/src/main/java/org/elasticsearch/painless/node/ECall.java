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
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubDefNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

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
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to method call [" + methodName + "/" + argumentNodes.size() + "]"));
        }

        Output output = new Output();
        Class<?> valueType;

        semanticScope.setCondition(prefixNode, Read.class);
        Output prefixOutput = prefixNode.analyze(classNode, semanticScope);
        Class<?> prefixValueType = semanticScope.getDecoration(prefixNode, ValueType.class).getValueType();

        if (prefixOutput.partialCanonicalTypeName != null) {
            throw createError(new IllegalArgumentException("cannot resolve symbol [" + prefixOutput.partialCanonicalTypeName + "]"));
        }

        ExpressionNode expressionNode;

        if (prefixValueType == def.class) {
            if (prefixOutput.isStaticType) {
                throw createError(new IllegalArgumentException("value required: " +
                        "instead found unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType) + "]"));
            }

            List<Output> argumentOutputs = new ArrayList<>(argumentNodes.size());

            for (AExpression argument : argumentNodes) {
                semanticScope.setCondition(argument, Read.class);
                semanticScope.setCondition(argument, Internal.class);
                Output expressionOutput = analyze(argument, classNode, semanticScope);
                Class<?> argumentValueType = semanticScope.getDecoration(argument, ValueType.class).getValueType();
                argumentOutputs.add(expressionOutput);

                if (argumentValueType == void.class) {
                    throw createError(new IllegalArgumentException(
                            "Argument(s) cannot be of [void] type when calling method [" + methodName + "]."));
                }
            }

            TargetType targetType = semanticScope.getDecoration(this, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(this, Explicit.class) ? def.class : targetType.getTargetType();

            CallSubDefNode callSubDefNode = new CallSubDefNode();

            for (Output argumentOutput : argumentOutputs) {
                callSubDefNode.addArgumentNode(argumentOutput.expressionNode);
            }

            callSubDefNode.setLocation(getLocation());
            callSubDefNode.setExpressionType(valueType);
            callSubDefNode.setName(methodName);
            expressionNode = callSubDefNode;
        } else {
            PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                    prefixValueType, prefixOutput.isStaticType, methodName, argumentNodes.size());

            if (method == null) {
                throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(prefixValueType) + ", " +
                        "" + methodName + "/" + argumentNodes.size() + "] not found"));
            }

            semanticScope.getScriptScope().markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            List<Output> argumentOutputs = new ArrayList<>(argumentNodes.size());
            List<PainlessCast> argumentCasts = new ArrayList<>(argumentOutputs.size());

            for (int argument = 0; argument < argumentNodes.size(); ++argument) {
                AExpression expression = argumentNodes.get(argument);

                semanticScope.setCondition(expression, Read.class);
                semanticScope.putDecoration(expression, new TargetType(method.typeParameters.get(argument)));
                semanticScope.setCondition(expression, Internal.class);
                Output expressionOutput = analyze(expression, classNode, semanticScope);
                argumentOutputs.add(expressionOutput);
                argumentCasts.add(expression.cast(semanticScope));
            }

            valueType = method.returnType;

            CallSubNode callSubNode = new CallSubNode();

            for (int argument = 0; argument < argumentNodes.size(); ++argument) {
                callSubNode.addArgumentNode(cast(argumentOutputs.get(argument).expressionNode, argumentCasts.get(argument)));
            }

            callSubNode.setLocation(getLocation());
            callSubNode.setExpressionType(valueType);
            callSubNode.setMethod(method);
            callSubNode.setBox(prefixValueType);
            expressionNode = callSubNode;
        }

        if (isNullSafe) {
            if (valueType.isPrimitive()) {
                throw new IllegalArgumentException("Result of null safe operator must be nullable");
            }

            NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
            nullSafeSubNode.setChildNode(expressionNode);
            nullSafeSubNode.setLocation(getLocation());
            nullSafeSubNode.setExpressionType(valueType);
            expressionNode = nullSafeSubNode;
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        CallNode callNode = new CallNode();
        callNode.setLeftNode(prefixOutput.expressionNode);
        callNode.setRightNode(expressionNode);
        callNode.setLocation(getLocation());
        callNode.setExpressionType(valueType);
        output.expressionNode = callNode;

        return output;
    }
}

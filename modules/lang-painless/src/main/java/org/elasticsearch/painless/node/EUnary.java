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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a unary math expression.
 */
public class EUnary extends AExpression {

    private final AExpression childNode;
    private final Operation operation;

    public EUnary(int identifier, Location location, AExpression childNode, Operation operation) {
        super(identifier, location);

        this.childNode = Objects.requireNonNull(childNode);
        this.operation = Objects.requireNonNull(operation);
    }

    public AExpression getChildNode() {
        return childNode;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        Output output = new Output();
        Class<?> valueType;

        Class<?> promote = null;

        Output childOutput;

        if ((operation == Operation.SUB || operation == Operation.ADD) &&
                (childNode instanceof ENumeric || childNode instanceof EDecimal)) {
            semanticScope.setCondition(childNode, Read.class);
            semanticScope.copyDecoration(this, childNode, TargetType.class);
            semanticScope.replicateCondition(this, childNode, Explicit.class);
            semanticScope.replicateCondition(this, childNode, Internal.class);

            if (childNode instanceof ENumeric) {
                ENumeric numeric = (ENumeric)childNode;

                if (operation == Operation.SUB) {
                    childOutput = numeric.analyze(semanticScope, numeric.getNumeric().charAt(0) != '-');
                } else {
                    childOutput = childNode.analyze(classNode, semanticScope);
                }
            } else if (childNode instanceof EDecimal) {
                EDecimal decimal = (EDecimal)childNode;

                if (operation == Operation.SUB) {
                    childOutput = decimal.analyze(semanticScope, decimal.getDecimal().charAt(0) != '-');
                } else {
                    childOutput = childNode.analyze(classNode, semanticScope);
                }
            } else {
                throw createError(new IllegalArgumentException("illegal tree structure"));
            }

            valueType = semanticScope.getDecoration(childNode, ValueType.class).getValueType();
            output.expressionNode = childOutput.expressionNode;
        } else {
            PainlessCast childCast;

            if (operation == Operation.NOT) {
                semanticScope.setCondition(childNode, Read.class);
                semanticScope.putDecoration(childNode, new TargetType(boolean.class));
                childOutput = analyze(childNode, classNode, semanticScope);
                childCast = childNode.cast(semanticScope);

                valueType = boolean.class;
            } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
                semanticScope.setCondition(childNode, Read.class);
                childOutput = analyze(childNode, classNode, semanticScope);
                Class<?> childValueType = semanticScope.getDecoration(childNode, ValueType.class).getValueType();

                promote = AnalyzerCaster.promoteNumeric(childValueType, operation != Operation.BWNOT);

                if (promote == null) {
                    throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                            "[" + operation.symbol + "] to the type " +
                            "[" + PainlessLookupUtility.typeToCanonicalTypeName(childValueType) + "]"));
                }

                semanticScope.putDecoration(childNode, new TargetType(promote));
                childCast = childNode.cast(semanticScope);

                TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

                if (promote == def.class && targetType != null) {
                    valueType = targetType.getTargetType();
                } else {
                    valueType = promote;
                }
            } else {
                throw createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
            }

            UnaryMathNode unaryMathNode = new UnaryMathNode();
            unaryMathNode.setChildNode(cast(childOutput.expressionNode, childCast));
            unaryMathNode.setLocation(getLocation());
            unaryMathNode.setExpressionType(valueType);
            unaryMathNode.setUnaryType(promote);
            unaryMathNode.setOperation(operation);
            unaryMathNode.setOriginallyExplicit(semanticScope.getCondition(this, Explicit.class));

            output.expressionNode = unaryMathNode;
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        return output;
    }
}

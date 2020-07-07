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
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a binary math expression.
 */
public class EBinary extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;
    private final Operation operation;

    public EBinary(int identifier, Location location, AExpression leftNode, AExpression rightNode, Operation operation) {
        super(identifier, location);

        this.operation = Objects.requireNonNull(operation);
        this.leftNode = Objects.requireNonNull(leftNode);
        this.rightNode = Objects.requireNonNull(rightNode);
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitBinary(this, input);
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

        Class<?> promote;
        Class<?> shiftDistance = null;

        semanticScope.setCondition(leftNode, Read.class);
        Output leftOutput = analyze(leftNode, classNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);
        Output rightOutput = analyze(rightNode, classNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

        Output output = new Output();
        Class<?> valueType;
        PainlessCast leftCast = null;
        PainlessCast rightCast = null;

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            semanticScope.putDecoration(leftNode, new TargetType(String.class));
            semanticScope.putDecoration(rightNode, new TargetType(Pattern.class));
            leftCast = leftNode.cast(semanticScope);
            rightCast = rightNode.cast(semanticScope);
            promote = boolean.class;
            valueType = boolean.class;
        } else {
            if (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.ADD) {
                promote = AnalyzerCaster.promoteAdd(leftValueType, rightValueType);
            } else if (operation == Operation.SUB) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightValueType, false);

                if (shiftDistance == null) {
                    promote = null;
                }
            } else if (operation == Operation.BWOR || operation == Operation.BWAND) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, false);
            } else if (operation == Operation.XOR) {
                promote = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else {
                throw createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
            }

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                        "[" + operation.symbol + "] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            valueType = promote;

            if (operation == Operation.ADD && promote == String.class) {
                if (leftOutput.expressionNode instanceof BinaryMathNode) {
                    BinaryMathNode binaryMathNode = (BinaryMathNode)leftOutput.expressionNode;

                    if (binaryMathNode.getOperation() == Operation.ADD && leftValueType == String.class) {
                        ((BinaryMathNode)leftOutput.expressionNode).setCat(true);
                    }
                }

                if (rightOutput.expressionNode instanceof BinaryMathNode) {
                    BinaryMathNode binaryMathNode = (BinaryMathNode)rightOutput.expressionNode;

                    if (binaryMathNode.getOperation() == Operation.ADD && rightValueType == String.class) {
                        ((BinaryMathNode)rightOutput.expressionNode).setCat(true);
                    }
                }
            } else if (promote == def.class || shiftDistance == def.class) {
                TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

                if (targetType != null) {
                    valueType = targetType.getTargetType();
                }
            } else {
                semanticScope.putDecoration(leftNode, new TargetType(promote));

                if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                    if (shiftDistance == long.class) {
                        semanticScope.putDecoration(rightNode, new TargetType(int.class));
                        semanticScope.setCondition(rightNode, Explicit.class);
                    } else {
                        semanticScope.putDecoration(rightNode, new TargetType(shiftDistance));
                    }
                } else {
                    semanticScope.putDecoration(rightNode, new TargetType(promote));
                }

                leftCast = leftNode.cast(semanticScope);
                rightCast = rightNode.cast(semanticScope);
            }
        }

        semanticScope.putDecoration(this, new ValueType(valueType));

        BinaryMathNode binaryMathNode = new BinaryMathNode();
        binaryMathNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        binaryMathNode.setRightNode(cast(rightOutput.expressionNode, rightCast));
        binaryMathNode.setLocation(getLocation());
        binaryMathNode.setExpressionType(valueType);
        binaryMathNode.setBinaryType(promote);
        binaryMathNode.setShiftType(shiftDistance);
        binaryMathNode.setOperation(operation);
        binaryMathNode.setCat(false);
        binaryMathNode.setOriginallyExplicit(semanticScope.getCondition(this, Explicit.class));
        output.expressionNode = binaryMathNode;

        return output;
    }
}

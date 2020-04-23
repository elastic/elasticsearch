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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.BinaryType;
import org.elasticsearch.painless.symbol.Decorations.Concatenate;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ShiftType;
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

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, EBinary userBinaryNode, SemanticScope semanticScope) {

        Operation operation = userBinaryNode.getOperation();

        if (semanticScope.getCondition(userBinaryNode, Write.class)) {
            throw userBinaryNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userBinaryNode, Read.class) == false) {
            throw userBinaryNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userLeftNode = userBinaryNode.getLeftNode();
        semanticScope.setCondition(userLeftNode, Read.class);
        visitor.checkedVisit(userLeftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userLeftNode, ValueType.class).getValueType();

        AExpression userRightNode = userBinaryNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);
        visitor.checkedVisit(userRightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();
        
        Class<?> valueType;
        Class<?> binaryType;
        Class<?> shiftType = null;

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            semanticScope.putDecoration(userLeftNode, new TargetType(String.class));
            semanticScope.putDecoration(userRightNode, new TargetType(Pattern.class));
            visitor.decorateWithCast(userLeftNode, semanticScope);
            visitor.decorateWithCast(userRightNode, semanticScope);
            binaryType = boolean.class;
            valueType = boolean.class;
        } else {
            if (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.ADD) {
                binaryType = AnalyzerCaster.promoteAdd(leftValueType, rightValueType);
            } else if (operation == Operation.SUB) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftType = AnalyzerCaster.promoteNumeric(rightValueType, false);

                if (shiftType == null) {
                    binaryType = null;
                }
            } else if (operation == Operation.BWOR || operation == Operation.BWAND) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, false);
            } else if (operation == Operation.XOR) {
                binaryType = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else {
                throw userBinaryNode.createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
            }

            if (binaryType == null) {
                throw userBinaryNode.createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                        "[" + operation.symbol + "] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            valueType = binaryType;

            if (operation == Operation.ADD && binaryType == String.class) {
                if (userLeftNode instanceof EBinary &&
                        ((EBinary)userLeftNode).getOperation() == Operation.ADD && leftValueType == String.class) {
                    semanticScope.setCondition(userLeftNode, Concatenate.class);
                }
                
                if (userRightNode instanceof EBinary &&
                        ((EBinary)userRightNode).getOperation() == Operation.ADD && rightValueType == String.class) {
                    semanticScope.setCondition(userRightNode, Concatenate.class);
                }
            } else if (binaryType == def.class || shiftType == def.class) {
                TargetType targetType = semanticScope.getDecoration(userBinaryNode, TargetType.class);

                if (targetType != null) {
                    valueType = targetType.getTargetType();
                }
            } else {
                semanticScope.putDecoration(userLeftNode, new TargetType(binaryType));

                if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                    if (shiftType == long.class) {
                        semanticScope.putDecoration(userRightNode, new TargetType(int.class));
                        semanticScope.setCondition(userRightNode, Explicit.class);
                    } else {
                        semanticScope.putDecoration(userRightNode, new TargetType(shiftType));
                    }
                } else {
                    semanticScope.putDecoration(userRightNode, new TargetType(binaryType));
                }

                visitor.decorateWithCast(userLeftNode, semanticScope);
                visitor.decorateWithCast(userRightNode, semanticScope);
            }
        }

        semanticScope.putDecoration(userBinaryNode, new ValueType(valueType));
        semanticScope.putDecoration(userBinaryNode, new BinaryType(binaryType));

        if (shiftType != null) {
            semanticScope.putDecoration(userBinaryNode, new ShiftType(shiftType));
        }
    }
}

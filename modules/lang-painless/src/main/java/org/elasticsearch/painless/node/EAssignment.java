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
import org.elasticsearch.painless.ir.AssignmentNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BraceNode;
import org.elasticsearch.painless.ir.BraceSubDefNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DotNode;
import org.elasticsearch.painless.ir.DotSubDefNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.Decorations.DefOptimized;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents an assignment with the lhs and rhs as child nodes.
 */
public class EAssignment extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;
    private final boolean postIfRead;
    private final Operation operation;

    public EAssignment(int identifier, Location location,
            AExpression leftNode, AExpression rightNode, boolean postIfRead, Operation operation) {

        super(identifier, location);

        this.leftNode = Objects.requireNonNull(leftNode);
        this.rightNode = Objects.requireNonNull(rightNode);
        this.postIfRead = postIfRead;
        this.operation = operation;
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    public boolean postIfRead() {
        return postIfRead;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope) {
        Output output = new Output();

        boolean cat = false;
        Class<?> promote = null;
        Class<?> shiftDistance = null;
        PainlessCast rightCast = null;
        PainlessCast there = null;
        PainlessCast back = null;

        semanticScope.replicateCondition(this, leftNode, Read.class);
        semanticScope.setCondition(leftNode, Write.class);
        Output leftOutput = analyze(leftNode, classNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, Decorations.ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);
        Output rightOutput;

        if (operation != null) {
            rightOutput = analyze(rightNode, classNode, semanticScope);
            Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();
            boolean shift = false;

            if (operation == Operation.MUL) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.DIV) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.REM) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.ADD) {
                promote = AnalyzerCaster.promoteAdd(leftValueType, rightValueType);
            } else if (operation == Operation.SUB) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.LSH) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightValueType, false);
                shift = true;
            } else if (operation == Operation.RSH) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightValueType, false);
                shift = true;
            } else if (operation == Operation.USH) {
                promote = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightValueType, false);
                shift = true;
            } else if (operation == Operation.BWAND) {
                promote = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else if (operation == Operation.XOR) {
                promote = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else if (operation == Operation.BWOR) {
                promote = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            if (promote == null || (shift && shiftDistance == null)) {
                throw createError(new ClassCastException("Cannot apply compound assignment " +
                        "[" + operation.symbol + "=] to types [" + leftValueType + "] and [" + rightValueType + "]."));
            }

            cat = operation == Operation.ADD && promote == String.class;

            if (cat && rightOutput.expressionNode instanceof BinaryMathNode) {
                BinaryMathNode binaryMathNode = (BinaryMathNode)rightOutput.expressionNode;

                if (binaryMathNode.getOperation() == Operation.ADD && rightValueType == String.class) {
                    ((BinaryMathNode)rightOutput.expressionNode).setCat(true);
                }
            }

            if (shift) {
                if (promote == def.class) {
                    // shifts are promoted independently, but for the def type, we need object.
                    semanticScope.putDecoration(rightNode, new TargetType(def.class));
                } else if (shiftDistance == long.class) {
                    semanticScope.putDecoration(rightNode, new TargetType(int.class));
                    semanticScope.setCondition(rightNode, Explicit.class);
                } else {
                    semanticScope.putDecoration(rightNode, new TargetType(shiftDistance));
                }
            } else {
                semanticScope.putDecoration(rightNode, new TargetType(promote));
            }

            rightCast = rightNode.cast(semanticScope);

            there = AnalyzerCaster.getLegalCast(getLocation(), leftValueType, promote, false, false);
            back = AnalyzerCaster.getLegalCast(getLocation(), promote, leftValueType, true, false);
        } else {
            // If the lhs node is a def optimized node we update the actual type to remove the need for a cast.
            if (semanticScope.getCondition(leftNode, DefOptimized.class)) {
                rightOutput = analyze(rightNode, classNode, semanticScope);
                Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

                if (rightValueType == void.class) {
                    throw createError(new IllegalArgumentException("Right-hand side cannot be a [void] type for assignment."));
                }

                leftValueType = rightValueType;
                leftOutput.expressionNode.setExpressionType(rightValueType);
                ExpressionNode expressionNode = leftOutput.expressionNode;

                if (expressionNode instanceof DotNode && ((DotNode)expressionNode).getRightNode() instanceof DotSubDefNode) {
                    ((DotNode)expressionNode).getRightNode().setExpressionType(leftValueType);
                } else if (expressionNode instanceof BraceNode && ((BraceNode)expressionNode).getRightNode() instanceof BraceSubDefNode) {
                    ((BraceNode)expressionNode).getRightNode().setExpressionType(leftValueType);
                }
            // Otherwise, we must adapt the rhs type to the lhs type with a cast.
            } else {
                semanticScope.putDecoration(rightNode, new TargetType(leftValueType));
                rightOutput = analyze(rightNode, classNode, semanticScope);
                rightCast = rightNode.cast(semanticScope);
            }
        }

        boolean read = semanticScope.getCondition(this, Read.class);
        ValueType valueType = new ValueType(read ? leftValueType : void.class);
        semanticScope.putDecoration(this, valueType);

        AssignmentNode assignmentNode = new AssignmentNode();

        assignmentNode.setLeftNode(leftOutput.expressionNode);
        assignmentNode.setRightNode(cast(rightOutput.expressionNode, rightCast));

        assignmentNode.setLocation(getLocation());
        assignmentNode.setExpressionType(valueType.getValueType());
        assignmentNode.setCompoundType(promote);
        assignmentNode.setPost(postIfRead);
        assignmentNode.setOperation(operation);
        assignmentNode.setRead(read);
        assignmentNode.setCat(cat);
        assignmentNode.setThere(there);
        assignmentNode.setBack(back);

        output.expressionNode = assignmentNode;

        return output;
    }
}

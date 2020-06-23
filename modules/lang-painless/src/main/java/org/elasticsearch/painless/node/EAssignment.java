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
import org.elasticsearch.painless.symbol.SemanticScope;
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
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        Output output = new Output();

        boolean cat = false;
        Class<?> promote = null;
        Class<?> shiftDistance = null;
        PainlessCast rightCast;
        PainlessCast there = null;
        PainlessCast back = null;

        Input leftInput = new Input();
        leftInput.read = input.read;
        leftInput.write = true;
        Output leftOutput = analyze(leftNode, classNode, semanticScope, leftInput);

        Input rightInput = new Input();
        Output rightOutput;

        if (operation != null) {
            rightOutput = analyze(rightNode, classNode, semanticScope, rightInput);
            boolean shift = false;

            if (operation == Operation.MUL) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.DIV) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.REM) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.ADD) {
                promote = AnalyzerCaster.promoteAdd(leftOutput.actual, rightOutput.actual);
            } else if (operation == Operation.SUB) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.LSH) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightOutput.actual, false);
                shift = true;
            } else if (operation == Operation.RSH) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightOutput.actual, false);
                shift = true;
            } else if (operation == Operation.USH) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightOutput.actual, false);
                shift = true;
            } else if (operation == Operation.BWAND) {
                promote = AnalyzerCaster.promoteXor(leftOutput.actual, rightOutput.actual);
            } else if (operation == Operation.XOR) {
                promote = AnalyzerCaster.promoteXor(leftOutput.actual, rightOutput.actual);
            } else if (operation == Operation.BWOR) {
                promote = AnalyzerCaster.promoteXor(leftOutput.actual, rightOutput.actual);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            if (promote == null || (shift && shiftDistance == null)) {
                throw createError(new ClassCastException("Cannot apply compound assignment " +
                        "[" + operation.symbol + "=] to types [" + leftOutput.actual + "] and [" + rightOutput.actual + "]."));
            }

            cat = operation == Operation.ADD && promote == String.class;

            if (cat && rightOutput.expressionNode instanceof BinaryMathNode) {
                BinaryMathNode binaryMathNode = (BinaryMathNode)rightOutput.expressionNode;

                if (binaryMathNode.getOperation() == Operation.ADD && rightOutput.actual == String.class) {
                    ((BinaryMathNode)rightOutput.expressionNode).setCat(true);
                }
            }

            if (shift) {
                if (promote == def.class) {
                    // shifts are promoted independently, but for the def type, we need object.
                    rightInput.expected = promote;
                } else if (shiftDistance == long.class) {
                    rightInput.expected = int.class;
                    rightInput.explicit = true;
                } else {
                    rightInput.expected = shiftDistance;
                }
            } else {
                rightInput.expected = promote;
            }

            rightCast = AnalyzerCaster.getLegalCast(rightNode.getLocation(),
                    rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);

            there = AnalyzerCaster.getLegalCast(getLocation(), leftOutput.actual, promote, false, false);
            back = AnalyzerCaster.getLegalCast(getLocation(), promote, leftOutput.actual, true, false);
        } else {
            // If the lhs node is a def optimized node we update the actual type to remove the need for a cast.
            if (leftOutput.isDefOptimized) {
                rightOutput = analyze(rightNode, classNode, semanticScope, rightInput);

                if (rightOutput.actual == void.class) {
                    throw createError(new IllegalArgumentException("Right-hand side cannot be a [void] type for assignment."));
                }

                rightInput.expected = rightOutput.actual;
                leftOutput.actual = rightOutput.actual;
                leftOutput.expressionNode.setExpressionType(rightOutput.actual);

                ExpressionNode expressionNode = leftOutput.expressionNode;

                if (expressionNode instanceof DotNode && ((DotNode)expressionNode).getRightNode() instanceof DotSubDefNode) {
                    ((DotNode)expressionNode).getRightNode().setExpressionType(leftOutput.actual);
                } else if (expressionNode instanceof BraceNode && ((BraceNode)expressionNode).getRightNode() instanceof BraceSubDefNode) {
                    ((BraceNode)expressionNode).getRightNode().setExpressionType(leftOutput.actual);
                }
            // Otherwise, we must adapt the rhs type to the lhs type with a cast.
            } else {
                rightInput.expected = leftOutput.actual;
                rightOutput = analyze(rightNode, classNode, semanticScope, rightInput);
            }

            rightCast = AnalyzerCaster.getLegalCast(rightNode.getLocation(),
                    rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);
        }

        output.actual = input.read ? leftOutput.actual : void.class;

        AssignmentNode assignmentNode = new AssignmentNode();

        assignmentNode.setLeftNode(leftOutput.expressionNode);
        assignmentNode.setRightNode(cast(rightOutput.expressionNode, rightCast));

        assignmentNode.setLocation(getLocation());
        assignmentNode.setExpressionType(output.actual);
        assignmentNode.setCompoundType(promote);
        assignmentNode.setPost(postIfRead);
        assignmentNode.setOperation(operation);
        assignmentNode.setRead(input.read);
        assignmentNode.setCat(cat);
        assignmentNode.setThere(there);
        assignmentNode.setBack(back);

        output.expressionNode = assignmentNode;

        return output;
    }
}

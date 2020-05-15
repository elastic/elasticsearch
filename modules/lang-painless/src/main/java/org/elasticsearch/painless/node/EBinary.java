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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a binary math expression.
 */
public class EBinary extends AExpression {

    protected final Operation operation;
    protected final AExpression left;
    protected final AExpression right;

    public EBinary(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        Class<?> promote = null;            // promoted type
        Class<?> shiftDistance = null;      // for shifts, the rhs is promoted independently
        boolean originallyExplicit = input.explicit; // record whether there was originally an explicit cast

        Input leftInput = new Input();
        Output leftOutput = analyze(left, classNode, scriptRoot, scope, leftInput);

        Output output = new Output();
        Input rightInput = new Input();
        Output rightOutput = analyze(right, classNode, scriptRoot, scope, rightInput);

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            leftInput.expected = String.class;
            rightInput.expected = Pattern.class;
            promote = boolean.class;
            output.actual = boolean.class;
        } else {
            if (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.ADD) {
                promote = AnalyzerCaster.promoteAdd(leftOutput.actual, rightOutput.actual);
            } else if (operation == Operation.SUB) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, false);
                shiftDistance = AnalyzerCaster.promoteNumeric(rightOutput.actual, false);

                if (shiftDistance == null) {
                    promote = null;
                }
            } else if (operation == Operation.BWOR || operation == Operation.BWAND) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, false);
            } else if (operation == Operation.XOR) {
                promote = AnalyzerCaster.promoteXor(leftOutput.actual, rightOutput.actual);
            } else {
                throw createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
            }

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                        "[" + operation.symbol + "] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftOutput.actual) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightOutput.actual) + "]"));
            }

            output.actual = promote;

            if (operation == Operation.ADD && promote == String.class) {
                leftInput.expected = leftOutput.actual;
                rightInput.expected = rightOutput.actual;

                if (leftOutput.expressionNode instanceof BinaryMathNode) {
                    BinaryMathNode binaryMathNode = (BinaryMathNode)leftOutput.expressionNode;

                    if (binaryMathNode.getOperation() == Operation.ADD && leftOutput.actual == String.class) {
                        ((BinaryMathNode)leftOutput.expressionNode).setCat(true);
                    }
                }

                if (rightOutput.expressionNode instanceof BinaryMathNode) {
                    BinaryMathNode binaryMathNode = (BinaryMathNode)rightOutput.expressionNode;

                    if (binaryMathNode.getOperation() == Operation.ADD && rightOutput.actual == String.class) {
                        ((BinaryMathNode)rightOutput.expressionNode).setCat(true);
                    }
                }
            } else if (promote == def.class || shiftDistance == def.class) {
                leftInput.expected = leftOutput.actual;
                rightInput.expected = rightOutput.actual;

                if (input.expected != null) {
                    output.actual = input.expected;
                }
            } else {
                leftInput.expected = promote;

                if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                    if (shiftDistance == long.class) {
                        rightInput.expected = int.class;
                        rightInput.explicit = true;
                    } else {
                        rightInput.expected = shiftDistance;
                    }
                } else {
                    rightInput.expected = promote;
                }
            }
        }

        PainlessCast leftCast = AnalyzerCaster.getLegalCast(left.location,
                leftOutput.actual, leftInput.expected, leftInput.explicit, leftInput.internal);
        PainlessCast rightCast = AnalyzerCaster.getLegalCast(right.location,
                rightOutput.actual, rightInput.expected, rightInput.explicit, rightInput.internal);

        BinaryMathNode binaryMathNode = new BinaryMathNode();

        binaryMathNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        binaryMathNode.setRightNode(cast(rightOutput.expressionNode, rightCast));

        binaryMathNode.setLocation(location);
        binaryMathNode.setExpressionType(output.actual);
        binaryMathNode.setBinaryType(promote);
        binaryMathNode.setShiftType(shiftDistance);
        binaryMathNode.setOperation(operation);
        binaryMathNode.setCat(false);
        binaryMathNode.setOriginallExplicit(originallyExplicit);

        output.expressionNode = binaryMathNode;

        return output;
    }
}

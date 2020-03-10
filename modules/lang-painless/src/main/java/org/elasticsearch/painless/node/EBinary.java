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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a binary math expression.
 */
public final class EBinary extends AExpression {

    final Operation operation;
    private AExpression left;
    private AExpression right;

    private Class<?> promote = null;            // promoted type
    private Class<?> shiftDistance = null;      // for shifts, the rhs is promoted independently
    boolean cat = false;
    private boolean originallyExplicit = false; // record whether there was originally an explicit cast

    public EBinary(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        originallyExplicit = input.explicit;

        Output leftOutput = left.analyze(scriptRoot, scope, new Input());
        Output rightOutput = right.analyze(scriptRoot, scope, new Input());

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            left.input.expected = String.class;
            right.input.expected = Pattern.class;
            promote = boolean.class;
            output.actual = boolean.class;
        } else {
            if (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM || operation == Operation.SUB) {
                promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);
            } else if (operation == Operation.ADD) {
                promote = AnalyzerCaster.promoteAdd(leftOutput.actual, rightOutput.actual);
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
                left.input.expected = leftOutput.actual;
                right.input.expected = rightOutput.actual;

                if (left instanceof EBinary && ((EBinary) left).operation == Operation.ADD && leftOutput.actual == String.class) {
                    ((EBinary) left).cat = true;
                }

                if (right instanceof EBinary && ((EBinary) right).operation == Operation.ADD && rightOutput.actual == String.class) {
                    ((EBinary) right).cat = true;
                }
            } else if (promote == def.class || shiftDistance != null && shiftDistance == def.class) {
                left.input.expected = leftOutput.actual;
                right.input.expected = rightOutput.actual;

                if (input.expected != null) {
                    output.actual = input.expected;
                }
            } else {
                left.input.expected = promote;

                if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                    if (shiftDistance == long.class) {
                        right.input.expected = int.class;
                        right.input.explicit = true;
                    } else {
                        right.input.expected = shiftDistance;
                    }
                } else {
                    right.input.expected = promote;
                }
            }
        }

        left.cast();
        right.cast();

        return output;
    }

    @Override
    BinaryMathNode write(ClassNode classNode) {
        BinaryMathNode binaryMathNode = new BinaryMathNode();

        binaryMathNode.setLeftNode(left.cast(left.write(classNode)));
        binaryMathNode.setRightNode(right.cast(right.write(classNode)));

        binaryMathNode.setLocation(location);
        binaryMathNode.setExpressionType(output.actual);
        binaryMathNode.setBinaryType(promote);
        binaryMathNode.setShiftType(shiftDistance);
        binaryMathNode.setOperation(operation);
        binaryMathNode.setCat(cat);
        binaryMathNode.setOriginallExplicit(originallyExplicit);

        return binaryMathNode;
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}

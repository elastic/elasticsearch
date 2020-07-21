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

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        semanticScope.setCondition(leftNode, Read.class);
        analyze(leftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);
        analyze(rightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();
        
        Class<?> valueType;
        Class<?> promote;
        Class<?> shiftDistance = null;

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            semanticScope.putDecoration(leftNode, new TargetType(String.class));
            semanticScope.putDecoration(rightNode, new TargetType(Pattern.class));
            leftNode.cast(semanticScope);
            rightNode.cast(semanticScope);
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
                if (leftNode instanceof EBinary &&
                        ((EBinary)leftNode).getOperation() == Operation.ADD && leftValueType == String.class) {
                    semanticScope.setCondition(leftNode, Concatenate.class);
                }
                
                if (rightNode instanceof EBinary &&
                        ((EBinary)rightNode).getOperation() == Operation.ADD && rightValueType == String.class) {
                    semanticScope.setCondition(rightNode, Concatenate.class);
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

                leftNode.cast(semanticScope);
                rightNode.cast(semanticScope);
            }
        }

        semanticScope.putDecoration(this, new ValueType(valueType));
        semanticScope.putDecoration(this, new BinaryType(promote));

        if (shiftDistance != null) {
            semanticScope.putDecoration(this, new ShiftType(shiftDistance));
        }
    }
}

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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.Concatenate;
import org.elasticsearch.painless.symbol.Decorations.DefOptimized;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.UpcastPainlessCast;
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
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitAssignment(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        semanticScope.replicateCondition(this, leftNode, Read.class);
        semanticScope.setCondition(leftNode, Write.class);
        analyze(leftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, Decorations.ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);

        if (operation != null) {
            analyze(rightNode, semanticScope);
            Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

            Class<?> promote;
            Class<?> shiftDistance = null;
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

            boolean cat = operation == Operation.ADD && promote == String.class;

            if (cat && rightNode instanceof EBinary &&
                    ((EBinary)rightNode).getOperation() == Operation.ADD && rightValueType == String.class) {
                semanticScope.setCondition(rightNode, Concatenate.class);
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

            rightNode.cast(semanticScope);

            PainlessCast upcast = AnalyzerCaster.getLegalCast(getLocation(), leftValueType, promote, false, false);
            PainlessCast downcast = AnalyzerCaster.getLegalCast(getLocation(), promote, leftValueType, true, false);

            semanticScope.putDecoration(this, new CompoundType(promote));

            if (cat) {
                semanticScope.setCondition(this, Concatenate.class);
            }

            if (upcast != null) {
                semanticScope.putDecoration(this, new UpcastPainlessCast(upcast));
            }

            if (downcast != null) {
                semanticScope.putDecoration(this, new DowncastPainlessCast(downcast));
            }
        // If the lhs node is a def optimized node we update the actual type to remove the need for a cast.
        } else if (semanticScope.getCondition(leftNode, DefOptimized.class)) {
            analyze(rightNode, semanticScope);
            Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

            if (rightValueType == void.class) {
                throw createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign type [" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]"));
            }

            semanticScope.putDecoration(leftNode, new ValueType(rightValueType));
            leftValueType = rightValueType;
        // Otherwise, we must adapt the rhs type to the lhs type with a cast.
        } else {
            semanticScope.putDecoration(rightNode, new TargetType(leftValueType));
            analyze(rightNode, semanticScope);
            rightNode.cast(semanticScope);
        }

        semanticScope.putDecoration(this, new ValueType(semanticScope.getCondition(this, Read.class) ? leftValueType : void.class));
    }
}

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
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import static java.util.Objects.requireNonNull;

/**
 * The Elvis operator ({@code ?:}), a null coalescing operator. Binary operator that evaluates the first expression and return it if it is
 * non null. If the first expression is null then it evaluates the second expression and returns it.
 */
public class EElvis extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;

    public EElvis(int identifier, Location location, AExpression leftNode, AExpression rightNode) {
        super(identifier, location);

        this.leftNode = requireNonNull(leftNode);
        this.rightNode = requireNonNull(rightNode);
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitElvis(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to elvis operation [?:]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from elvis operation [?:]"));
        }

        TargetType targetType = semanticScope.getDecoration(this, TargetType.class);

        if (targetType != null && targetType.getTargetType().isPrimitive()) {
            throw createError(new IllegalArgumentException("Elvis operator cannot return primitives"));
        }

        Class<?> valueType;

        semanticScope.setCondition(leftNode, Read.class);
        semanticScope.copyDecoration(this, leftNode, TargetType.class);
        semanticScope.replicateCondition(this, leftNode, Explicit.class);
        semanticScope.replicateCondition(this, leftNode, Internal.class);
        analyze(leftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);
        semanticScope.copyDecoration(this, rightNode, TargetType.class);
        semanticScope.replicateCondition(this, rightNode, Explicit.class);
        semanticScope.replicateCondition(this, rightNode, Internal.class);
        analyze(rightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

        if (leftNode instanceof ENull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is null."));
        }
        if (    leftNode instanceof EBooleanConstant ||
                leftNode instanceof ENumeric         ||
                leftNode instanceof EDecimal         ||
                leftNode instanceof EString
        ) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a constant."));
        }
        if (leftValueType.isPrimitive()) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }
        if (rightNode instanceof ENull) {
            throw createError(new IllegalArgumentException("Extraneous elvis operator. RHS is null."));
        }

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            semanticScope.putDecoration(leftNode, new TargetType(promote));
            semanticScope.putDecoration(rightNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }

        leftNode.cast(semanticScope);
        rightNode.cast(semanticScope);

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}

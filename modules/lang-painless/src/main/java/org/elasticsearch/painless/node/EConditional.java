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
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents a conditional expression.
 */
public class EConditional extends AExpression {

    private final AExpression conditionNode;
    private final AExpression leftNode;
    private final AExpression rightNode;

    public EConditional(int identifier, Location location, AExpression conditionNode, AExpression leftNode, AExpression rightNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.leftNode = Objects.requireNonNull(leftNode);
        this.rightNode = Objects.requireNonNull(rightNode);
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    @Override
    Output analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to conditional operation [?:]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from conditional operation [?:]"));
        }

        Output output = new Output();


        semanticScope.setCondition(conditionNode, Read.class);
        semanticScope.putDecoration(conditionNode, new TargetType(boolean.class));
        Output conditionOutput = analyze(conditionNode, semanticScope);
        PainlessCast conditionCast = conditionNode.cast(semanticScope);

        semanticScope.setCondition(leftNode, Read.class);
        semanticScope.copyDecoration(this, leftNode, TargetType.class);
        semanticScope.replicateCondition(this, leftNode, Explicit.class);
        semanticScope.replicateCondition(this, leftNode, Internal.class);
        Output leftOutput = analyze(leftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(leftNode, ValueType.class).getValueType();

        semanticScope.setCondition(rightNode, Read.class);
        semanticScope.copyDecoration(this, rightNode, TargetType.class);
        semanticScope.replicateCondition(this, rightNode, Explicit.class);
        semanticScope.replicateCondition(this, rightNode, Internal.class);
        Output rightOutput = analyze(rightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(rightNode, ValueType.class).getValueType();

        TargetType targetType = semanticScope.getDecoration(this, TargetType.class);
        Class<?> valueType;

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the conditional operator [?:] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            semanticScope.putDecoration(leftNode, new TargetType(promote));
            semanticScope.putDecoration(rightNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }

        PainlessCast leftCast = leftNode.cast(semanticScope);
        PainlessCast rightCast = rightNode.cast(semanticScope);

        semanticScope.putDecoration(this, new ValueType(valueType));

        ConditionalNode conditionalNode = new ConditionalNode();
        conditionalNode.setLeftNode(cast(leftOutput.expressionNode, leftCast));
        conditionalNode.setRightNode(cast(rightOutput.expressionNode, rightCast));
        conditionalNode.setConditionNode(cast(conditionOutput.expressionNode, conditionCast));
        conditionalNode.setLocation(getLocation());
        conditionalNode.setExpressionType(valueType);
        output.expressionNode = conditionalNode;

        return output;
    }
}

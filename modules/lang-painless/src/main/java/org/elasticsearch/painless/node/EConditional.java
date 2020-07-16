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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
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
    private final AExpression trueNode;
    private final AExpression falseNode;

    public EConditional(int identifier, Location location, AExpression conditionNode, AExpression trueNode, AExpression falseNode) {
        super(identifier, location);

        this.conditionNode = Objects.requireNonNull(conditionNode);
        this.trueNode = Objects.requireNonNull(trueNode);
        this.falseNode = Objects.requireNonNull(falseNode);
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getTrueNode() {
        return trueNode;
    }

    public AExpression getFalseNode() {
        return falseNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitConditional(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, EConditional userConditionalNode, SemanticScope semanticScope) {

        if (semanticScope.getCondition(userConditionalNode, Write.class)) {
            throw userConditionalNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to conditional operation [?:]"));
        }

        if (semanticScope.getCondition(userConditionalNode, Read.class) == false) {
            throw userConditionalNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from conditional operation [?:]"));
        }

        AExpression userConditionNode = userConditionalNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        visitor.checkedVisit(userConditionNode, semanticScope);
        visitor.decorateWithCast(userConditionNode, semanticScope);

        AExpression userTrueNode = userConditionalNode.getTrueNode();
        semanticScope.setCondition(userTrueNode, Read.class);
        semanticScope.copyDecoration(userConditionalNode, userTrueNode, TargetType.class);
        semanticScope.replicateCondition(userConditionalNode, userTrueNode, Explicit.class);
        semanticScope.replicateCondition(userConditionalNode, userTrueNode, Internal.class);
        visitor.checkedVisit(userTrueNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userTrueNode, ValueType.class).getValueType();

        AExpression userFalseNode = userConditionalNode.getFalseNode();
        semanticScope.setCondition(userFalseNode, Read.class);
        semanticScope.copyDecoration(userConditionalNode, userFalseNode, TargetType.class);
        semanticScope.replicateCondition(userConditionalNode, userFalseNode, Explicit.class);
        semanticScope.replicateCondition(userConditionalNode, userFalseNode, Internal.class);
        visitor.checkedVisit(userFalseNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userFalseNode, ValueType.class).getValueType();

        TargetType targetType = semanticScope.getDecoration(userConditionalNode, TargetType.class);
        Class<?> valueType;

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            if (promote == null) {
                throw userConditionalNode.createError(new ClassCastException("cannot apply the conditional operator [?:] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            semanticScope.putDecoration(userTrueNode, new TargetType(promote));
            semanticScope.putDecoration(userFalseNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }

        visitor.decorateWithCast(userTrueNode, semanticScope);
        visitor.decorateWithCast(userFalseNode, semanticScope);

        semanticScope.putDecoration(userConditionalNode, new ValueType(valueType));
    }
}

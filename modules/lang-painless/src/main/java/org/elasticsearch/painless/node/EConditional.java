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

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to conditional operation [?:]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from conditional operation [?:]"));
        }

        semanticScope.setCondition(conditionNode, Read.class);
        semanticScope.putDecoration(conditionNode, new TargetType(boolean.class));
        analyze(conditionNode, semanticScope);
        conditionNode.cast(semanticScope);
        
        semanticScope.setCondition(trueNode, Read.class);
        semanticScope.copyDecoration(this, trueNode, TargetType.class);
        semanticScope.replicateCondition(this, trueNode, Explicit.class);
        semanticScope.replicateCondition(this, trueNode, Internal.class);
        analyze(trueNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(trueNode, ValueType.class).getValueType();

        semanticScope.setCondition(falseNode, Read.class);
        semanticScope.copyDecoration(this, falseNode, TargetType.class);
        semanticScope.replicateCondition(this, falseNode, Explicit.class);
        semanticScope.replicateCondition(this, falseNode, Internal.class);
        analyze(falseNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(falseNode, ValueType.class).getValueType();

        TargetType targetType = semanticScope.getDecoration(this, TargetType.class);
        Class<?> valueType;

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            if (promote == null) {
                throw createError(new ClassCastException("cannot apply the conditional operator [?:] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            semanticScope.putDecoration(trueNode, new TargetType(promote));
            semanticScope.putDecoration(falseNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }
        
        trueNode.cast(semanticScope);
        falseNode.cast(semanticScope);

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}

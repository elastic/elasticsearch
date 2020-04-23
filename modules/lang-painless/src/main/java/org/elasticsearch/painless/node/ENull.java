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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.DefaultSemanticAnalysisPhase;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

/**
 * Represents a null constant.
 */
public class ENull extends AExpression {

    public ENull(int identifier, Location location) {
        super(identifier, location);
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitNull(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ENull userNullNode, SemanticScope semanticScope) {

        if (semanticScope.getCondition(userNullNode, Write.class)) {
            throw userNullNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to null constant"));
        }

        if (semanticScope.getCondition(userNullNode, Read.class) == false) {
            throw userNullNode.createError(new IllegalArgumentException("not a statement: null constant not used"));
        }

        TargetType targetType = semanticScope.getDecoration(userNullNode, TargetType.class);
        Class<?> valueType;

        if (targetType != null) {
            if (targetType.getTargetType().isPrimitive()) {
                throw userNullNode.createError(new IllegalArgumentException(
                        "Cannot cast null to a primitive type [" + targetType.getTargetCanonicalTypeName() + "]."));
            }

            valueType = targetType.getTargetType();
        } else {
            valueType = Object.class;
        }

        semanticScope.putDecoration(userNullNode, new ValueType(valueType));
    }
}

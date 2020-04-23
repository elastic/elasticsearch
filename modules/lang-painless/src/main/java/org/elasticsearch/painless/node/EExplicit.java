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
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Objects;

/**
 * Represents an explicit cast.
 */
public class EExplicit extends AExpression {

    private final String canonicalTypeName;
    private final AExpression childNode;

    public EExplicit(int identifier, Location location, String canonicalTypeName, AExpression childNode) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.childNode = Objects.requireNonNull(childNode);
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public AExpression getChildNode() {
        return childNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitExplicit(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, EExplicit userExplicitNode, SemanticScope semanticScope) {

        String canonicalTypeName = userExplicitNode.getCanonicalTypeName();

        if (semanticScope.getCondition(userExplicitNode, Write.class)) {
            throw userExplicitNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to an explicit cast with target type [" + canonicalTypeName + "]"));
        }

        if (semanticScope.getCondition(userExplicitNode, Read.class) == false) {
            throw userExplicitNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from explicit cast with target type [" + canonicalTypeName + "]"));
        }

        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userExplicitNode.createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        AExpression userChildNode = userExplicitNode.getChildNode();
        semanticScope.setCondition(userChildNode, Read.class);
        semanticScope.putDecoration(userChildNode, new TargetType(valueType));
        semanticScope.setCondition(userChildNode, Explicit.class);
        visitor.checkedVisit(userChildNode, semanticScope);
        visitor.decorateWithCast(userChildNode, semanticScope);

        semanticScope.putDecoration(userExplicitNode, new ValueType(valueType));
    }
}

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
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents an array instantiation.
 */
public class ENewArray extends AExpression {

    private final String canonicalTypeName;
    private final List<AExpression> valueNodes;
    private final boolean isInitializer;

    public ENewArray(int identifier, Location location, String canonicalTypeName, List<AExpression> valueNodes, boolean isInitializer) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
        this.isInitializer = isInitializer;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    public boolean isInitializer() {
        return isInitializer;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitNewArray(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, ENewArray userNewArrayNode, SemanticScope semanticScope) {

        if (semanticScope.getCondition(userNewArrayNode, Write.class)) {
            throw userNewArrayNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to new array"));
        }

        if (semanticScope.getCondition(userNewArrayNode, Read.class) == false) {
            throw userNewArrayNode.createError(new IllegalArgumentException("not a statement: result not used from new array"));
        }

        String canonicalTypeName = userNewArrayNode.getCanonicalTypeName();
        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userNewArrayNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        for (AExpression userValueNode : userNewArrayNode.getValueNodes()) {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode,
                    new TargetType(userNewArrayNode.isInitializer() ? valueType.getComponentType() : int.class));
            semanticScope.setCondition(userValueNode, Internal.class);
            visitor.checkedVisit(userValueNode, semanticScope);
            visitor.decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.putDecoration(userNewArrayNode, new ValueType(valueType));
    }
}

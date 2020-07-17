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

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to an explicit cast with target type [" + canonicalTypeName + "]"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: result not used from explicit cast with target type [" + canonicalTypeName + "]"));
        }

        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        semanticScope.setCondition(childNode, Read.class);
        semanticScope.putDecoration(childNode, new TargetType(valueType));
        semanticScope.setCondition(childNode, Explicit.class);
        analyze(childNode, semanticScope);
        childNode.cast(semanticScope);

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}

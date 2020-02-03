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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a single variable declaration.
 */
public final class SDeclaration extends AStatement {

    private DType type;
    protected final String name;
    private AExpression expression;

    public SDeclaration(Location location, DType type, String name, AExpression expression) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.expression = expression;
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        DResolvedType resolvedType = type.resolveType(scriptRoot.getPainlessLookup());
        type = resolvedType;

        if (expression != null) {
            expression.expected = resolvedType.getType();
            expression.analyze(scriptRoot, scope);
            expression = expression.cast(scriptRoot, scope);
        }

        scope.defineVariable(location, resolvedType.getType(), name, false);
    }

    @Override
    DeclarationNode write(ClassNode classNode) {
        DeclarationNode declarationNode = new DeclarationNode();

        declarationNode.setExpressionNode(expression == null ? null : expression.write(classNode));

        declarationNode.setLocation(location);
        declarationNode.setDeclarationType(((DResolvedType)type).getType());
        declarationNode.setName(name);

        return declarationNode;
    }

    @Override
    public String toString() {
        if (expression == null) {
            return singleLineToString(type, name);
        }
        return singleLineToString(type, name, expression);
    }
}

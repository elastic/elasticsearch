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
public class SDeclaration extends AStatement {

    protected final DType type;
    protected final String name;
    protected final boolean requiresDefault;
    protected final AExpression expression;

    public SDeclaration(Location location, DType type, String name, boolean requiresDefault, AExpression expression) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.name = Objects.requireNonNull(name);
        this.requiresDefault = requiresDefault;
        this.expression = expression;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        DResolvedType resolvedType = type.resolveType(scriptRoot.getPainlessLookup());

        AExpression.Output expressionOutput = null;

        if (expression != null) {
            AExpression.Input expressionInput = new AExpression.Input();
            expressionInput.expected = resolvedType.getType();
            expressionOutput = expression.analyze(classNode, scriptRoot, scope, expressionInput);
            expression.cast(expressionInput, expressionOutput);
        }

        scope.defineVariable(location, resolvedType.getType(), name, false);

        DeclarationNode declarationNode = new DeclarationNode();
        declarationNode.setExpressionNode(expression == null ? null : expression.cast(expressionOutput));
        declarationNode.setLocation(location);
        declarationNode.setDeclarationType(resolvedType.getType());
        declarationNode.setName(name);
        declarationNode.setRequiresDefault(requiresDefault);

        output.statementNode = declarationNode;

        return output;
    }
}

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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a single variable declaration.
 */
public class SDeclaration extends AStatement {

    private final DType type;
    private final String symbol;
    private final boolean requiresDefault;
    private final AExpression valueNode;

    public SDeclaration(int identifier, Location location, DType type, String symbol, boolean requiresDefault, AExpression valueNode) {
        super(identifier, location);

        this.type = Objects.requireNonNull(type);
        this.symbol = Objects.requireNonNull(symbol);
        this.requiresDefault = requiresDefault;
        this.valueNode = valueNode;
    }

    public DType getType() {
        return type;
    }

    public String getSymbol() {
        return symbol;
    }

    public boolean requiresDefault() {
        return requiresDefault;
    }

    public AExpression getValueNode() {
        return valueNode;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (scriptRoot.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw createError(new IllegalArgumentException("invalid declaration: type [" + symbol + "] cannot be a name"));
        }

        DResolvedType resolvedType = type.resolveType(scriptRoot.getPainlessLookup());

        AExpression.Output expressionOutput = null;
        PainlessCast expressionCast = null;

        if (valueNode != null) {
            AExpression.Input expressionInput = new AExpression.Input();
            expressionInput.expected = resolvedType.getType();
            expressionOutput = AExpression.analyze(valueNode, classNode, scriptRoot, scope, expressionInput);
            expressionCast = AnalyzerCaster.getLegalCast(valueNode.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal);
        }

        scope.defineVariable(getLocation(), resolvedType.getType(), symbol, false);

        DeclarationNode declarationNode = new DeclarationNode();
        declarationNode.setExpressionNode(valueNode == null ? null :
                AExpression.cast(expressionOutput.expressionNode, expressionCast));
        declarationNode.setLocation(getLocation());
        declarationNode.setDeclarationType(resolvedType.getType());
        declarationNode.setName(symbol);
        declarationNode.setRequiresDefault(requiresDefault);

        Output output = new Output();
        output.statementNode = declarationNode;

        return output;
    }
}

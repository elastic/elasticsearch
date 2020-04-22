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
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Objects;

/**
 * Represents a single variable declaration.
 */
public class SDeclaration extends AStatement {

    private final String canonicalTypeName;
    private final String symbol;
    private final AExpression valueNode;

    public SDeclaration(int identifier, Location location, String canonicalTypeName, String symbol, AExpression valueNode) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.valueNode = valueNode;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public AExpression getValueNode() {
        return valueNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitDeclaration(this, input);
    }

    public static void visitDefaultSemanticAnalysis(
            DefaultSemanticAnalysisPhase visitor, SDeclaration userDeclarationNode, SemanticScope semanticScope) {

        ScriptScope scriptScope = semanticScope.getScriptScope();
        String symbol = userDeclarationNode.getSymbol();

        if (scriptScope.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw userDeclarationNode.createError(new IllegalArgumentException(
                    "invalid declaration: type [" + symbol + "] cannot be a name"));
        }

        String canonicalTypeName = userDeclarationNode.getCanonicalTypeName();
        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw userDeclarationNode.createError(new IllegalArgumentException(
                    "invalid declaration: cannot resolve type [" + canonicalTypeName + "]"));
        }

        AExpression userValueNode = userDeclarationNode.getValueNode();

        if (userValueNode != null) {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(type));
            visitor.checkedVisit(userValueNode, semanticScope);
            visitor.decorateWithCast(userValueNode, semanticScope);
        }

        Location location = userDeclarationNode.getLocation();
        Variable variable = semanticScope.defineVariable(location, type, symbol, false);
        semanticScope.putDecoration(userDeclarationNode, new SemanticVariable(variable));
    }
}

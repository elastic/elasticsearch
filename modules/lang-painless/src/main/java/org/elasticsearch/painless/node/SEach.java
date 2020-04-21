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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.AnyContinue;
import org.elasticsearch.painless.symbol.Decorations.BeginLoop;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a for-each loop and defers to subnodes depending on type.
 */
public class SEach extends AStatement {

    private final String canonicalTypeName;
    private final String symbol;
    private final AExpression iterableNode;
    private final SBlock blockNode;

    public SEach(int identifier, Location location, String canonicalTypeName, String symbol, AExpression iterableNode, SBlock blockNode) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.iterableNode = Objects.requireNonNull(iterableNode);
        this.blockNode = blockNode;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public AExpression getIterableNode() {
        return iterableNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitEach(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        semanticScope.setCondition(iterableNode, Read.class);
        AExpression.analyze(iterableNode, semanticScope);
        Class<?> iterableValueType = semanticScope.getDecoration(iterableNode, ValueType.class).getValueType();

        Class<?> clazz = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        semanticScope = semanticScope.newLocalScope();
        Variable variable = semanticScope.defineVariable(getLocation(), clazz, symbol, true);
        semanticScope.putDecoration(this, new SemanticVariable(variable));

        if (blockNode == null) {
            throw createError(new IllegalArgumentException("Extraneous for each loop."));
        }

        semanticScope.setCondition(blockNode, BeginLoop.class);
        semanticScope.setCondition(blockNode, InLoop.class);
        blockNode.analyze(semanticScope);

        if (semanticScope.getCondition(blockNode, LoopEscape.class) &&
                semanticScope.getCondition(blockNode, AnyContinue.class) == false) {
            throw createError(new IllegalArgumentException("Extraneous for loop."));
        }

        if (iterableValueType.isArray()) {
            PainlessCast painlessCast =
                    AnalyzerCaster.getLegalCast(getLocation(), iterableValueType.getComponentType(), variable.getType(), true, true);

            if (painlessCast != null) {
                semanticScope.putDecoration(this, new ExpressionPainlessCast(painlessCast));
            }
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            if (iterableValueType != def.class) {
                PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().
                        lookupPainlessMethod(iterableValueType, false, "iterator", 0);

                if (method == null) {
                    throw createError(new IllegalArgumentException(
                            "method [" + typeToCanonicalTypeName(iterableValueType) + ", iterator/0] not found"));
                }

                semanticScope.putDecoration(this, new IterablePainlessMethod(method));
            }

            PainlessCast painlessCast = AnalyzerCaster.getLegalCast(getLocation(), def.class, variable.getType(), true, true);

            if (painlessCast != null) {
                semanticScope.putDecoration(this, new ExpressionPainlessCast(painlessCast));
            }
        } else {
            throw createError(new IllegalArgumentException("Illegal for each type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(iterableValueType) + "]."));
        }
    }
}

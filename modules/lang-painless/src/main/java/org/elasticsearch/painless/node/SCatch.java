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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.AnyBreak;
import org.elasticsearch.painless.symbol.Decorations.AnyContinue;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.LastLoop;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Objects;

/**
 * Represents a catch block as part of a try-catch block.
 */
public class SCatch extends AStatement {

    private final Class<?> baseException;
    private final String canonicalTypeName;
    private final String symbol;
    private final SBlock blockNode;

    public SCatch(int identifier, Location location, Class<?> baseException, String canonicalTypeName, String symbol, SBlock blockNode) {
        super(identifier, location);

        this.baseException = Objects.requireNonNull(baseException);
        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.blockNode = blockNode;
    }

    public Class<?> getBaseException() {
        return baseException;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitCatch(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        ScriptScope scriptScope = semanticScope.getScriptScope();

        if (scriptScope.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw createError(new IllegalArgumentException("invalid declaration: type [" + symbol + "] cannot be a name"));
        }

        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        Variable variable = semanticScope.defineVariable(getLocation(), type, symbol, false);
        semanticScope.putDecoration(this, new SemanticVariable(variable));

        if (baseException.isAssignableFrom(type) == false) {
            throw createError(new ClassCastException(
                    "cannot cast from [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] " +
                    "to [" + PainlessLookupUtility.typeToCanonicalTypeName(baseException) + "]"));
        }

        if (blockNode != null) {
            semanticScope.replicateCondition(this, blockNode, LastSource.class);
            semanticScope.replicateCondition(this, blockNode, InLoop.class);
            semanticScope.replicateCondition(this, blockNode, LastLoop.class);
            blockNode.analyze(semanticScope);

            semanticScope.setCondition(this, MethodEscape.class);
            semanticScope.setCondition(this, LoopEscape.class);
            semanticScope.setCondition(this, AllEscape.class);
            semanticScope.setCondition(this, AnyContinue.class);
            semanticScope.setCondition(this, AnyBreak.class);
        }
    }
}

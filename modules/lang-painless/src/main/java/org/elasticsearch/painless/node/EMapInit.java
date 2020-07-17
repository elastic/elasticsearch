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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessConstructor;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.SemanticScope;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a map initialization shortcut.
 */
public class EMapInit extends AExpression {

    private final List<AExpression> keyNodes;
    private final List<AExpression> valueNodes;

    public EMapInit(int identifier, Location location, List<AExpression> keyNodes, List<AExpression> valueNodes) {
        super(identifier, location);

        this.keyNodes = Collections.unmodifiableList(Objects.requireNonNull(keyNodes));
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
    }

    public List<AExpression> getKeyNodes() {
        return keyNodes;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitMapInit(this, input);
    }

    @Override
    void analyze(SemanticScope semanticScope) {
        if (semanticScope.getCondition(this, Write.class)) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to map initializer"));
        }

        if (semanticScope.getCondition(this, Read.class) == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from map initializer"));
        }

        Class<?> valueType = HashMap.class;

        PainlessConstructor constructor = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessConstructor(valueType, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(valueType) + ", <init>/0] not found"));
        }

        semanticScope.putDecoration(this, new StandardPainlessConstructor(constructor));

        PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(valueType, false, "put", 2);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(valueType) + ", put/2] not found"));
        }

        semanticScope.putDecoration(this, new StandardPainlessMethod(method));

        if (keyNodes.size() != valueNodes.size()) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        for (int i = 0; i < keyNodes.size(); ++i) {
            AExpression expression = keyNodes.get(i);
            semanticScope.setCondition(expression, Read.class);
            semanticScope.putDecoration(expression, new TargetType(def.class));
            semanticScope.setCondition(expression, Internal.class);
            analyze(expression, semanticScope);
            expression.cast(semanticScope);

            expression = valueNodes.get(i);
            semanticScope.setCondition(expression, Read.class);
            semanticScope.putDecoration(expression, new TargetType(def.class));
            semanticScope.setCondition(expression, Internal.class);
            analyze(expression, semanticScope);
            expression.cast(semanticScope);
        }

        semanticScope.putDecoration(this, new ValueType(valueType));
    }
}

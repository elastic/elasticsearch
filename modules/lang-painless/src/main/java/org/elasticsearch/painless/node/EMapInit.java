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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.util.ArrayList;
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
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to map initializer"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from map initializer"));
        }

        Output output = new Output();
        output.actual = HashMap.class;

        PainlessConstructor constructor = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessConstructor(output.actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/0] not found"));
        }

        PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(output.actual, false, "put", 2);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(output.actual) + ", put/2] not found"));
        }

        if (keyNodes.size() != valueNodes.size()) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        List<Output> keyOutputs = new ArrayList<>(keyNodes.size());
        List<PainlessCast> keyCasts = new ArrayList<>(keyNodes.size());
        List<Output> valueOutputs = new ArrayList<>(valueNodes.size());
        List<PainlessCast> valueCasts = new ArrayList<>(valueNodes.size());

        for (int i = 0; i < keyNodes.size(); ++i) {
            AExpression expression = keyNodes.get(i);
            Input expressionInput = new Input();
            expressionInput.expected = def.class;
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, semanticScope, expressionInput);
            keyOutputs.add(expressionOutput);
            keyCasts.add(AnalyzerCaster.getLegalCast(expression.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));

            expression = valueNodes.get(i);
            expressionInput = new Input();
            expressionInput.expected = def.class;
            expressionInput.internal = true;
            expressionOutput = analyze(expression, classNode, semanticScope, expressionInput);
            valueCasts.add(AnalyzerCaster.getLegalCast(expression.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));

            valueOutputs.add(expressionOutput);
        }

        MapInitializationNode mapInitializationNode = new MapInitializationNode();

        for (int i = 0; i < keyNodes.size(); ++i) {
            mapInitializationNode.addArgumentNode(
                    cast(keyOutputs.get(i).expressionNode, keyCasts.get(i)),
                    cast(valueOutputs.get(i).expressionNode, valueCasts.get(i)));
        }

        mapInitializationNode.setLocation(getLocation());
        mapInitializationNode.setExpressionType(output.actual);
        mapInitializationNode.setConstructor(constructor);
        mapInitializationNode.setMethod(method);

        output.expressionNode = mapInitializationNode;

        return output;
    }
}

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
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a list initialization shortcut.
 */
public class EListInit extends AExpression {

    private final List<AExpression> valueNodes;

    public EListInit(int identifier, Location location, List<AExpression> valueNodes) {
        super(identifier, location);

        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException("invalid assignment: cannot assign a value to list initializer"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: result not used from list initializer"));
        }

        Output output = new Output();
        output.actual = ArrayList.class;

        PainlessConstructor constructor = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessConstructor(output.actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(output.actual) + ", <init>/0] not found"));
        }

        PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(output.actual, false, "add", 1);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(output.actual) + ", add/1] not found"));
        }

        List<Output> valueOutputs = new ArrayList<>(valueNodes.size());
        List<PainlessCast> valueCasts = new ArrayList<>(valueNodes.size());

        for (AExpression expression : valueNodes) {
            Input expressionInput = new Input();
            expressionInput.expected = def.class;
            expressionInput.internal = true;
            Output expressionOutput = analyze(expression, classNode, semanticScope, expressionInput);
            valueOutputs.add(expressionOutput);
            valueCasts.add(AnalyzerCaster.getLegalCast(expression.getLocation(),
                    expressionOutput.actual, expressionInput.expected, expressionInput.explicit, expressionInput.internal));
        }

        ListInitializationNode listInitializationNode = new ListInitializationNode();

        for (int i = 0; i < valueNodes.size(); ++i) {
            listInitializationNode.addArgumentNode(cast(valueOutputs.get(i).expressionNode, valueCasts.get(i)));
        }

        listInitializationNode.setLocation(getLocation());
        listInitializationNode.setExpressionType(output.actual);
        listInitializationNode.setConstructor(constructor);
        listInitializationNode.setMethod(method);

        output.expressionNode = listInitializationNode;

        return output;
    }
}

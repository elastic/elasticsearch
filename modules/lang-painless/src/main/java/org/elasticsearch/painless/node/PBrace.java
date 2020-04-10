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
import org.elasticsearch.painless.ir.BraceNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents an array load/store and defers to a child subnode.
 */
public class PBrace extends AStoreable {

    protected final AExpression index;

    // TODO: #54015
    private boolean isDefOptimized = false;

    public PBrace(Location location, AExpression prefix, AExpression index) {
        super(location, prefix);

        this.index = Objects.requireNonNull(index);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, AExpression.Input input) {
        AStoreable.Input storeableInput = new AStoreable.Input();
        storeableInput.read = input.read;
        storeableInput.expected = input.expected;
        storeableInput.explicit = input.explicit;
        storeableInput.internal = input.internal;

        return analyze(classNode, scriptRoot, scope, storeableInput);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, AStoreable.Input input) {
        if (input.read == false && input.write == false) {
            throw createError(new IllegalArgumentException("not a statement: result of brace operator not used"));
        }

        Output output = new Output();

        Input prefixInput = new Input();
        Output prefixOutput = prefix.analyze(classNode, scriptRoot, scope, prefixInput);
        prefixInput.expected = prefixOutput.actual;
        prefix.cast(prefixInput, prefixOutput);

        AStoreable sub;

        if (prefixOutput.actual.isArray()) {
            sub = new PSubBrace(location, prefixOutput.actual, index);
        } else if (prefixOutput.actual == def.class) {
            sub = new PSubDefArray(location, index);
        } else if (Map.class.isAssignableFrom(prefixOutput.actual)) {
            sub = new PSubMapShortcut(location, prefixOutput.actual, index);
        } else if (List.class.isAssignableFrom(prefixOutput.actual)) {
            sub = new PSubListShortcut(location, prefixOutput.actual, index);
        } else {
            throw createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefixOutput.actual) + "]."));
        }

        isDefOptimized = sub.isDefOptimized();

        Input subInput = new Input();
        subInput.write = input.write;
        subInput.read = input.read;
        subInput.expected = input.expected;
        subInput.explicit = input.explicit;
        Output subOutput = sub.analyze(classNode, scriptRoot, scope, subInput);
        output.actual = subOutput.actual;

        BraceNode braceNode = new BraceNode();

        braceNode.setLeftNode(prefix.cast(prefixOutput));
        braceNode.setRightNode(subOutput.expressionNode);

        braceNode.setLocation(location);
        braceNode.setExpressionType(output.actual);

        output.expressionNode = braceNode;

        return output;
    }

    @Override
    boolean isDefOptimized() {
        return isDefOptimized;
    }
}

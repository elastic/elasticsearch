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
public final class PBrace extends AStoreable {

    private AExpression index;

    private AStoreable sub = null;

    public PBrace(Location location, AExpression prefix, AExpression index) {
        super(location, prefix);

        this.index = Objects.requireNonNull(index);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        prefix.analyze(scriptRoot, scope);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(scriptRoot, scope);

        if (prefix.actual.isArray()) {
            sub = new PSubBrace(location, prefix.actual, index);
        } else if (prefix.actual == def.class) {
            sub = new PSubDefArray(location, index);
        } else if (Map.class.isAssignableFrom(prefix.actual)) {
            sub = new PSubMapShortcut(location, prefix.actual, index);
        } else if (List.class.isAssignableFrom(prefix.actual)) {
            sub = new PSubListShortcut(location, prefix.actual, index);
        } else {
            throw createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefix.actual) + "]."));
        }

        sub.write = write;
        sub.read = read;
        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(scriptRoot, scope);
        actual = sub.actual;
    }

    @Override
    BraceNode write(ClassNode classNode) {
        BraceNode braceNode = new BraceNode();

        braceNode.setLeftNode(prefix.write(classNode));
        braceNode.setRightNode(sub.write(classNode));

        braceNode.setLocation(location);
        braceNode.setExpressionType(actual);

        return braceNode;
    }

    @Override
    boolean isDefOptimized() {
        return sub.isDefOptimized();
    }

    @Override
    void updateActual(Class<?> actual) {
        sub.updateActual(actual);
        this.actual = actual;
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}

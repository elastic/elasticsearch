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
import org.elasticsearch.painless.ir.BraceSubNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents an array load/store.
 */
final class PSubBrace extends AStoreable {

    private final Class<?> clazz;
    private AExpression index;

    PSubBrace(Location location, Class<?> clazz, AExpression index) {
        super(location);

        this.clazz = Objects.requireNonNull(clazz);
        this.index = Objects.requireNonNull(index);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        index.expected = int.class;
        index.analyze(scriptRoot, scope);
        index = index.cast(scriptRoot, scope);

        actual = clazz.getComponentType();
    }

    BraceSubNode write(ClassNode classNode) {
        BraceSubNode braceSubNode = new BraceSubNode();

        braceSubNode.setChildNode(index.write(classNode));

        braceSubNode.setLocation(location);
        braceSubNode.setExpressionType(actual);

        return braceSubNode;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Class<?> actual) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}

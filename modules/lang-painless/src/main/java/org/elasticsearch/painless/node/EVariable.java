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
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a variable load/store.
 */
public final class EVariable extends AStoreable {

    private final String name;

    public EVariable(Location location, String name) {
        super(location);

        this.name = Objects.requireNonNull(name);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        Variable variable = scope.getVariable(location, name);

        if (write && variable.isFinal()) {
            throw createError(new IllegalArgumentException("Variable [" + variable.getName() + "] is read-only."));
        }

        actual = variable.getType();
    }

    @Override
    VariableNode write(ClassNode classNode) {
        VariableNode variableNode = new VariableNode();

        variableNode.setLocation(location);
        variableNode.setExpressionType(actual);
        variableNode.setName(name);

        return variableNode;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Class<?> actual) {
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    public String toString() {
        return singleLineToString(name);
    }
}

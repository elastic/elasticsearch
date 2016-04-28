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

package org.elasticsearch.painless.tree.utility;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class Variables {
    public static class Variable {
        public final String location;
        public final String name;
        public final Type type;

        public boolean read = false;
        public int slot = -1;

        private Variable(final String location, final String name, final Type type) {
            this.location = location;
            this.name = name;
            this.type = type;
        }
    }

    private final List<Deque<List<Variable>>> scopes = new ArrayList<>();
    private int scopesIndex;

    public Variables() {
        final Deque<List<Variable>> scope = new ArrayDeque<>();
        scope.push(new ArrayList<>());
        scopes.add(scope);

        scopesIndex = 0;
    }

    public void incrementScope() {
        ++scopesIndex;

        final Deque<List<Variable>> scope = scopes.size() == scopesIndex ? new ArrayDeque<>() : scopes.get(scopesIndex);
        scope.add(new ArrayList<>());
        scopes.add(scope);
    }

    public void decrementScope() {
        final List<Variable> variables = scopes.get(scopesIndex).peek();

        for (final Variable variable : variables) {
            if (!variable.read) {
                throw new IllegalArgumentException("Error [" + variable.location + "]: Variable [" + variable.name + "] never used.");
            }
        }

        --scopesIndex;
    }

    public Variable getVariable(final String location, final String name) {
        for (int index = 0; index < scopesIndex; ++index) {
            final List<Variable> variables = scopes.get(index).peek();
            final Iterator<Variable> itr = variables.iterator();

            while (itr.hasNext()) {
                final Variable variable = itr.next();

                if (variable.name.equals(name)) {
                    return variable;
                }
            }
        }

        if (location != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable [" + name + "] not defined.");
        }

        return null;
    }

    public Variable addVariable(final Definition definition, final String location, final String typestr, final String name) {
        if (getVariable(null, name) != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable [" + name + "] already defined.");
        }

        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException("Error " + location + ": Not a type [" + typestr + "].");
        }

        boolean legal = name.contains("<");

        try {
            definition.getType(name);
            legal = false;
        } catch (final IllegalArgumentException exception) {
            // Do nothing.
        }

        if (!legal) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] cannot be a type.");
        }

        final Variable variable = new Variable(location, name, type);
        final List<Variable> variables = scopes.get(scopesIndex).peek();
        variables.add(variable);

        return variable;
    }

    public void removeSpecial(final String... names) {
        final List<String> remove = Arrays.asList(names);
        final List<Variable> variables = new ArrayList<>();
        final Deque<List<Variable>> scope = scopes.get(0);

        for (final Variable variable : scope.peek()) {
            if (!remove.contains(variable.name) || variable.read) {
                variables.add(variable);
            }
        }

        scope.pop();
        scope.push(variables);
    }

    public void buildSlots() {
        int slot = 0;

        for (final Deque<List<Variable>> scope : scopes) {
            final int scopeStart = slot;

            for (final List<Variable> variables : scope) {
                final int variableStart = slot;

                for (final Variable variable : variables) {
                    variable.slot = slot;
                    slot += variable.type.sort.size;

                }

                slot = variableStart;
            }

            slot = scopeStart;
        }
    }
}


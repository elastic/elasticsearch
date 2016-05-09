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

import org.elasticsearch.painless.compiler.CompilerSettings;
import org.elasticsearch.painless.compiler.Definition;
import org.elasticsearch.painless.compiler.Definition.Type;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

public final class Variables {
    public static final class Special {
        public boolean doc = false;
        public boolean score = false;
        public boolean loop = false;
    }

    public static final class Variable {
        public final String location;
        public final String name;
        public final Type type;
        public final int slot;
        public boolean read = false;

        private Variable(final String location, final String name, final Type type, final int slot) {
            this.location = location;
            this.name = name;
            this.type = type;
            this.slot = slot;
        }
    }

    private final Definition definition;

    private final Deque<Integer> scopes = new ArrayDeque<>();
    private final Deque<Variable> variables = new ArrayDeque<>();

    public Variables(final CompilerSettings settings, final Definition definition, final Special special) {
        this.definition = definition;

        incrementScope();

        addVariable("[ #this ]", "Executable", "#this");
        addVariable("[ input ]", "Map<String,def>", "input");

        if (special.score) {
            addVariable("[ score ]", "float", "score");
        }

        if (special.doc) {
            addVariable("[ doc ]", "Map<String,def>", "doc");
        }

        if (special.loop && settings.getMaxLoopCounter() > 0) {
            addVariable("[ #loop ]", "int", "#loop");
        }
    }

    public void incrementScope() {
        scopes.push(0);
    }

    public void decrementScope() {
        int remove = scopes.pop();

        while (remove > 0) {
            final Variable variable = variables.pop();

            if (variable.read) {
                throw new IllegalArgumentException("Error [" + variable.location + "]: Variable [" + variable.name + "] never used.");
            }

            --remove;
        }
    }

    public Variable getVariable(final String location, final String name) {
        final Iterator<Variable> itr = variables.iterator();

        while (itr.hasNext()) {
            final Variable variable = itr.next();

            if (variable.name.equals(name)) {
                return variable;
            }
        }

        if (location != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable [" + name + "] not defined.");
        }

        return null;
    }

    public Variable addVariable(final String location, final String typestr, final String name) {
        if (getVariable(null, name) != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable [" + name + "] already defined.");
        }

        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException("Error " + location + ": Not a type [" + typestr + "].");
        }

        boolean legal = !name.contains("<");

        try {
            definition.getType(name);
            legal = false;
        } catch (final IllegalArgumentException exception) {
            // Do nothing.
        }

        if (!legal) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] cannot be a type.");
        }

        final Variable previous = variables.peekFirst();
        int slot = 0;

        if (previous != null) {
            slot = previous.slot + previous.type.type.getSize();
        }

        final Variable variable = new Variable(location, name, type, slot);
        variables.push(variable);

        final int update = scopes.pop() + 1;
        scopes.push(update);

        return variable;
    }
}


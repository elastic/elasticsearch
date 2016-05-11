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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Type;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Tracks variables across compilation phases.
 */
public final class Variables {

    /**
     * Tracks reserved variables.  Must be given to any source of input
     * prior to beginning the analysis phase so that reserved variables
     * are known ahead of time to assign appropriate slots without
     * being wasteful.
     */
    public static final class Reserved {
        public static final String THIS = "#this";
        public static final String INPUT = "input";
        public static final String SCORE = "_score";
        public static final String DOC = "doc";
        public static final String CTX = "ctx";
        public static final String LOOP = "#loop";

        boolean score = false;
        boolean ctx = false;
        boolean loop = false;

        public void markSpecial(final String name) {
            if (SCORE.equals(name)) {
                score = true;
            } else if (CTX.equals(name)) {
                ctx = true;
            }
        }

        public boolean isSpecial(final String name) {
            return name.equals(THIS) || name.equals(INPUT) || name.equals(SCORE) ||
                name.equals(DOC) || name.equals(CTX) || name.equals(LOOP);
         }

        public void usesLoop() {
            loop = true;
        }
    }

    public static final class Variable {
        public final String location;
        public final String name;
        public final Type type;
        public final int slot;
        public final boolean readonly;

        public boolean read = false;

        private Variable(final String location, final String name, final Type type, final int slot, final boolean readonly) {
            this.location = location;
            this.name = name;
            this.type = type;
            this.slot = slot;
            this.readonly = readonly;
        }
    }

    private final Definition definition;
    final Reserved reserved;

    private final Deque<Integer> scopes = new ArrayDeque<>();
    private final Deque<Variable> variables = new ArrayDeque<>();

    public Variables(final CompilerSettings settings, final Definition definition, final Reserved reserved) {
        this.definition = definition;
        this.reserved = reserved;

        incrementScope();

        addVariable("[" + Reserved.THIS + "]", definition.execType.name, Reserved.THIS, true, true);
        addVariable("[" + Reserved.INPUT + "]", definition.smapType.name, Reserved.INPUT, true, true);
        addVariable("[" + Reserved.SCORE + "]", definition.doubleType.name, Reserved.SCORE, true, true);
        addVariable("[" + Reserved.DOC + "]", definition.smapType.name, Reserved.DOC, true, true);

        if (reserved.ctx) {
            addVariable("[" + Reserved.CTX + "]", definition.smapType.name, Reserved.CTX, true, true);
        }

        if (reserved.loop && settings.getMaxLoopCounter() > 0) {
            addVariable("[" + Reserved.LOOP + "]", definition.intType.name, Reserved.LOOP, true, true);
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

    public Variable addVariable(final String location, final String typestr, final String name,
                                final boolean readonly, final boolean reserved) {
        if (!reserved && this.reserved.isSpecial(name)) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] is reserved.");
        }

        if (getVariable(null, name) != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] already defined.");
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

        final Variable variable = new Variable(location, name, type, slot, readonly);
        variables.push(variable);

        final int update = scopes.pop() + 1;
        scopes.push(update);

        return variable;
    }
}


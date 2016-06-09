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
        public static final String THIS   = "#this";
        public static final String PARAMS = "params";
        public static final String SCORER = "#scorer";
        public static final String DOC    = "doc";
        public static final String VALUE  = "_value";
        public static final String SCORE  = "_score";
        public static final String CTX    = "ctx";
        public static final String LOOP   = "#loop";

        boolean score = false;
        boolean ctx = false;
        boolean loop = false;

        public void markReserved(String name) {
            if (SCORE.equals(name)) {
                score = true;
            } else if (CTX.equals(name)) {
                ctx = true;
            }
        }

        public boolean isReserved(String name) {
            return name.equals(THIS) || name.equals(PARAMS) || name.equals(SCORER) || name.equals(DOC) ||
                name.equals(VALUE) || name.equals(SCORE) || name.equals(CTX) || name.equals(LOOP);
         }

        public void usesLoop() {
            loop = true;
        }
    }

    public static final class Variable {
        public final Location location;
        public final String name;
        public final Type type;
        public final int slot;
        public final boolean readonly;

        public boolean read = false;

        private Variable(Location location, String name, Type type, int slot, boolean readonly) {
            this.location = location;
            this.name = name;
            this.type = type;
            this.slot = slot;
            this.readonly = readonly;
        }
    }

    final Reserved reserved;

    // TODO: this datastructure runs in linear time for nearly all operations. use linkedhashset instead?
    private final Deque<Integer> scopes = new ArrayDeque<>();
    private final Deque<Variable> variables = new ArrayDeque<>();

    public Variables(Reserved reserved) {
        this.reserved = reserved;

        incrementScope();

        // Method variables.

        // This reference.  Internal use only.
        addVariable(null, Definition.getType("Object"), Reserved.THIS, true, true);

        // Input map of variables passed to the script.
        addVariable(null, Definition.getType("Map"), Reserved.PARAMS, true, true);

        // Scorer parameter passed to the script.  Internal use only.
        addVariable(null, Definition.DEF_TYPE, Reserved.SCORER, true, true);

        // Doc parameter passed to the script. TODO: Currently working as a Map, we can do better?
        addVariable(null, Definition.getType("Map"), Reserved.DOC, true, true);

        // Aggregation _value parameter passed to the script.
        addVariable(null, Definition.DEF_TYPE, Reserved.VALUE, true, true);

        // Shortcut variables.

        // Document's score as a read-only double.
        if (reserved.score) {
            addVariable(null, Definition.DOUBLE_TYPE, Reserved.SCORE, true, true);
        }

        // The ctx map set by executable scripts as a read-only map.
        if (reserved.ctx) {
            addVariable(null, Definition.getType("Map"), Reserved.CTX, true, true);
        }

        // Loop counter to catch infinite loops.  Internal use only.
        if (reserved.loop) {
            addVariable(null, Definition.INT_TYPE, Reserved.LOOP, true, true);
        }
    }

    public void incrementScope() {
        scopes.push(0);
    }

    public void decrementScope() {
        int remove = scopes.pop();

        while (remove > 0) {
            Variable variable = variables.pop();

            // TODO: is this working? the code reads backwards...
            if (variable.read) {
                throw variable.location.createError(new IllegalArgumentException("Variable [" + variable.name + "] never used."));
            }

            --remove;
        }
    }

    public Variable getVariable(Location location, String name) {
         Iterator<Variable> itr = variables.iterator();

        while (itr.hasNext()) {
             Variable variable = itr.next();

            if (variable.name.equals(name)) {
                return variable;
            }
        }

        throw location.createError(new IllegalArgumentException("Variable [" + name + "] not defined."));
    }

    private boolean variableExists(String name) {
        return variables.contains(name);
    }

    public Variable addVariable(Location location, Type type, String name, boolean readonly, boolean reserved) {
        if (!reserved && this.reserved.isReserved(name)) {
            throw location.createError(new IllegalArgumentException("Variable name [" + name + "] is reserved."));
        }

        if (variableExists(name)) {
            throw new IllegalArgumentException("Variable name [" + name + "] already defined.");
        }

        try {
            Definition.getType(name);
        } catch (IllegalArgumentException exception) {
            // Do nothing.
        }

        Variable previous = variables.peekFirst();
        int slot = 0;

        if (previous != null) {
            slot = previous.slot + previous.type.type.getSize();
        }

        Variable variable = new Variable(location, name, type, slot, readonly);
        variables.push(variable);

        int update = scopes.pop() + 1;
        scopes.push(update);

        return variable;
    }
}


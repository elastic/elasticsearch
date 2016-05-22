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
        public final String location;
        public final String name;
        public final Type type;
        public final int slot;
        public final boolean readonly;

        public boolean read = false;

        private Variable(String location, String name, Type type, int slot, boolean readonly) {
            this.location = location;
            this.name = name;
            this.type = type;
            this.slot = slot;
            this.readonly = readonly;
        }
    }

    final Reserved reserved;

    private final Deque<Integer> scopes = new ArrayDeque<>();
    private final Deque<Variable> variables = new ArrayDeque<>();

    public Variables(Reserved reserved) {
        this.reserved = reserved;

        incrementScope();

        // Method variables.

        // This reference.  Internal use only.
        addVariable("[" + Reserved.THIS + "]"  , "Executable", Reserved.THIS  , true, true);

        // Input map of variables passed to the script.
        addVariable("[" + Reserved.PARAMS + "]", "Map", Reserved.PARAMS, true, true);

        // Scorer parameter passed to the script.  Internal use only.
        addVariable("[" + Reserved.SCORER + "]", "def", Reserved.SCORER, true, true);

        // Doc parameter passed to the script. TODO: Currently working as a Map, we can do better?
        addVariable("[" + Reserved.DOC + "]"   , "Map", Reserved.DOC   , true, true);

        // Aggregation _value parameter passed to the script.
        addVariable("[" + Reserved.VALUE + "]" , "def", Reserved.VALUE , true, true);

        // Shortcut variables.

        // Document's score as a read-only double.
        if (reserved.score) {
            addVariable("[" + Reserved.SCORE + "]", "double", Reserved.SCORE, true, true);
        }

        // The ctx map set by executable scripts as a read-only map.
        if (reserved.ctx) {
            addVariable("[" + Reserved.CTX + "]", "Map", Reserved.CTX, true, true);
        }

        // Loop counter to catch infinite loops.  Internal use only.
        if (reserved.loop) {
            addVariable("[" + Reserved.LOOP + "]", "int", Reserved.LOOP, true, true);
        }
    }

    public void incrementScope() {
        scopes.push(0);
    }

    public void decrementScope() {
        int remove = scopes.pop();

        while (remove > 0) {
             Variable variable = variables.pop();

            if (variable.read) {
                throw new IllegalArgumentException("Error [" + variable.location + "]: Variable [" + variable.name + "] never used.");
            }

            --remove;
        }
    }

    public Variable getVariable(String location, String name) {
         Iterator<Variable> itr = variables.iterator();

        while (itr.hasNext()) {
             Variable variable = itr.next();

            if (variable.name.equals(name)) {
                return variable;
            }
        }

        if (location != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable [" + name + "] not defined.");
        }

        return null;
    }

    public Variable addVariable(String location, String typestr, String name, boolean readonly, boolean reserved) {
        if (!reserved && this.reserved.isReserved(name)) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] is reserved.");
        }

        if (getVariable(null, name) != null) {
            throw new IllegalArgumentException("Error " + location + ": Variable name [" + name + "] already defined.");
        }

        final Type type;

        try {
            type = Definition.getType(typestr);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("Error " + location + ": Not a type [" + typestr + "].");
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


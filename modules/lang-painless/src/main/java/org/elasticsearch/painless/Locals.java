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

import org.elasticsearch.painless.ScriptClassInfo.MethodArgument;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToJavaType;

/**
 * Tracks user defined variables across compilation phases.
 */
public final class Locals {

    /** Reserved word: loop counter */
    public static final String LOOP   = "#loop";
    /** Reserved word: unused */
    public static final String THIS   = "#this";

    /** Set of reserved keywords. */
    public static final Set<String> KEYWORDS = Set.of(THIS, LOOP);

    /** Creates a new local variable scope (e.g. loop) inside the current scope */
    public static Locals newLocalScope(Locals currentScope) {
        return new Locals(currentScope);
    }

    /**
     * Creates a new lambda scope inside the current scope
     * <p>
     * This is just like {@link #newFunctionScope}, except the captured parameters are made read-only.
     */
    public static Locals newLambdaScope(Locals programScope, String name, Class<?> returnType, List<Parameter> parameters,
                                        int captureCount, int maxLoopCounter) {
        Locals locals = new Locals(programScope, returnType, KEYWORDS);
        List<Class<?>> typeParameters = parameters.stream().map(parameter -> typeToJavaType(parameter.clazz)).collect(Collectors.toList());
        for (int i = 0; i < parameters.size(); i++) {
            Parameter parameter = parameters.get(i);
            // TODO: allow non-captures to be r/w:
            // boolean isCapture = i < captureCount;
            // currently, this cannot be allowed, as we swap in real types,
            // but that can prevent a store of a different type...
            boolean isCapture = true;
            locals.addVariable(parameter.location, parameter.clazz, parameter.name, isCapture);
        }
        // Loop counter to catch infinite loops.  Internal use only.
        if (maxLoopCounter > 0) {
            locals.defineVariable(null, int.class, LOOP, true);
        }
        return locals;
    }

    /** Creates a new function scope inside the current scope */
    public static Locals newFunctionScope(Locals programScope, Class<?> returnType, List<Parameter> parameters, int maxLoopCounter) {
        Locals locals = new Locals(programScope, returnType, KEYWORDS);
        for (Parameter parameter : parameters) {
            locals.addVariable(parameter.location, parameter.clazz, parameter.name, false);
        }
        // Loop counter to catch infinite loops.  Internal use only.
        if (maxLoopCounter > 0) {
            locals.defineVariable(null, int.class, LOOP, true);
        }
        return locals;
    }

    /** Creates a new main method scope */
    public static Locals newMainMethodScope(ScriptClassInfo scriptClassInfo, Locals programScope, int maxLoopCounter) {
        Locals locals = new Locals(programScope, scriptClassInfo.getExecuteMethodReturnType(), KEYWORDS);
        // This reference. Internal use only.
        locals.defineVariable(null, Object.class, THIS, true);

        // Method arguments
        for (MethodArgument arg : scriptClassInfo.getExecuteArguments()) {
            locals.defineVariable(null, arg.getClazz(), arg.getName(), true);
        }

        // Loop counter to catch infinite loops.  Internal use only.
        if (maxLoopCounter > 0) {
            locals.defineVariable(null, int.class, LOOP, true);
        }
        return locals;
    }

    /** Creates a new program scope as the root of all scopes */
    public static Locals newProgramScope() {
        return new Locals(null, null, null);
    }

    /** Checks if a variable exists or not, in this scope or any parents. */
    public boolean hasVariable(String name) {
        Variable variable = lookupVariable(null, name);
        if (variable != null) {
            return true;
        }
        if (parent != null) {
            return parent.hasVariable(name);
        }
        return false;
    }

    /** Accesses a variable. This will throw IAE if the variable does not exist */
    public Variable getVariable(Location location, String name) {
        Variable variable = lookupVariable(location, name);
        if (variable != null) {
            return variable;
        }
        if (parent != null) {
            return parent.getVariable(location, name);
        }
        throw location.createError(new IllegalArgumentException("Variable [" + name + "] is not defined."));
    }

    /** Creates a new variable. Throws IAE if the variable has already been defined (even in a parent) or reserved. */
    public Variable addVariable(Location location, Class<?> clazz, String name, boolean readonly) {
        if (hasVariable(name)) {
            throw location.createError(new IllegalArgumentException("Variable [" + name + "] is already defined."));
        }
        if (keywords.contains(name)) {
            throw location.createError(new IllegalArgumentException("Variable [" + name + "] is reserved."));
        }
        return defineVariable(location, clazz, name, readonly);
    }

    /** Return type of this scope (e.g. int, if inside a function that returns int) */
    public Class<?> getReturnType() {
        return returnType;
    }

    /** Returns the top-level program scope. */
    public Locals getProgramScope() {
        Locals locals = this;
        while (locals.getParent() != null) {
            locals = locals.getParent();
        }
        return locals;
    }

    ///// private impl

    // parent scope
    private final Locals parent;
    // return type of this scope
    private final Class<?> returnType;
    // keywords for this scope
    private final Set<String> keywords;
    // next slot number to assign
    private int nextSlotNumber;
    // variable name -> variable
    private Map<String,Variable> variables;

    /**
     * Create a new Locals
     */
    private Locals(Locals parent) {
        this(parent, parent.returnType, parent.keywords);
    }

    /**
     * Create a new Locals with specified return type
     */
    private Locals(Locals parent, Class<?> returnType, Set<String> keywords) {
        this.parent = parent;
        this.returnType = returnType;
        this.keywords = keywords;
        if (parent == null) {
            this.nextSlotNumber = 0;
        } else {
            this.nextSlotNumber = parent.getNextSlot();
        }
    }

    /** Returns the parent scope */
    private Locals getParent() {
        return parent;
    }

    /** Looks up a variable at this scope only. Returns null if the variable does not exist. */
    private Variable lookupVariable(Location location, String name) {
        if (variables == null) {
            return null;
        }
        return variables.get(name);
    }

    /** Defines a variable at this scope internally. */
    private Variable defineVariable(Location location, Class<?> type, String name, boolean readonly) {
        if (variables == null) {
            variables = new HashMap<>();
        }
        Variable variable = new Variable(location, name, type, getNextSlot(), readonly);
        variables.put(name, variable); // TODO: check result
        nextSlotNumber += MethodWriter.getType(type).getSize();
        return variable;
    }

    private int getNextSlot() {
        return nextSlotNumber;
    }

    public static final class Variable {
        public final Location location;
        public final String name;
        public final Class<?> clazz;
        public final boolean readonly;
        private final int slot;

        public Variable(Location location, String name, Class<?> clazz, int slot, boolean readonly) {
            this.location = location;
            this.name = name;
            this.clazz = clazz;
            this.slot = slot;
            this.readonly = readonly;
        }

        public int getSlot() {
            return slot;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            b.append("Variable[type=").append(PainlessLookupUtility.typeToCanonicalTypeName(clazz));
            b.append(",name=").append(name);
            b.append(",slot=").append(slot);
            if (readonly) {
                b.append(",readonly");
            }
            b.append(']');
            return b.toString();
        }
    }

    public static final class Parameter {
        public final Location location;
        public final String name;
        public final Class<?> clazz;

        public Parameter(Location location, String name, Class<?> clazz) {
            this.location = location;
            this.name = name;
            this.clazz = clazz;
        }
    }
}

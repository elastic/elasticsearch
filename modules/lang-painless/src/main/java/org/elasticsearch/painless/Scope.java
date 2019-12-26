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

import org.elasticsearch.painless.lookup.PainlessLookupUtility;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks user defined variables across compilation phases.
 */
public abstract class Scope {

    public static class Variable {

        protected final Class<?> type;
        protected final String name;
        protected final boolean isReadOnly;

        public Variable(Class<?> type, String name, boolean isReadOnly) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
            this.isReadOnly = isReadOnly;
        }

        public Class<?> getType() {
            return type;
        }

        public String getCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(type);
        }

        public String getName() {
            return name;
        }

        public boolean isReadOnly() {
            return isReadOnly;
        }
    }

    public static class FunctionScope extends Scope {

        protected final Class<?> returnType;
        protected final Set<String> areReadFrom = new HashSet<>();

        public FunctionScope(Class<?> returnType) {
            this.returnType = Objects.requireNonNull(returnType);
        }

        @Override
        public boolean isVariableDefined(String name) {
            return variables.containsKey(name);
        }

        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                throw location.createError(new IllegalArgumentException("variable [" + name + "] is not defined"));
            }

            areReadFrom.add(name);

            return variable;
        }

        @Override
        public Class<?> getReturnType() {
            return returnType;
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(returnType);
        }

        public Set<String> getReadFrom() {
            return Collections.unmodifiableSet(areReadFrom);
        }
    }

    public static class LambdaScope extends Scope {

        protected final Scope parent;
        protected final Class<?> returnType;
        protected final Set<Variable> captures = new HashSet<>();

        protected LambdaScope(Scope parent, Class<?> returnType) {
            this.parent = parent;
            this.returnType = returnType;
        }

        @Override
        public boolean isVariableDefined(String name) {
            if (variables.containsKey(name)) {
                return true;
            }

            return parent.isVariableDefined(name);
        }

        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                variable = parent.getVariable(location, name);
                variable = new Variable(variable.getType(), variable.getName(), true);
                captures.add(variable);
            }

            return variable;
        }

        @Override
        public Class<?> getReturnType() {
            return returnType;
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(returnType);
        }

        public Set<Variable> getCaptures() {
            return Collections.unmodifiableSet(captures);
        }
    }

    public static class LocalScope extends Scope {

        protected final Scope parent;

        protected LocalScope(Scope parent) {
            this.parent = parent;
        }

        @Override
        public boolean isVariableDefined(String name) {
            if (variables.containsKey(name)) {
                return true;
            }

            return parent.isVariableDefined(name);
        }

        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                variable = parent.getVariable(location, name);
            }

            return variable;
        }

        @Override
        public Class<?> getReturnType() {
            return parent.getReturnType();
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return parent.getReturnCanonicalTypeName();
        }
    }

    public static FunctionScope newFunctionScope(Class<?> returnType) {
        return new FunctionScope(returnType);
    }

    protected final Map<String, Variable> variables = new HashMap<>();

    protected Scope() {
        // do nothing
    }

    public LambdaScope newLambdaScope(Class<?> returnType) {
        return new LambdaScope(this, returnType);
    }

    public LocalScope newLocalScope() {
        return new LocalScope(this);
    }

    public abstract Class<?> getReturnType();
    public abstract String getReturnCanonicalTypeName();

    public Variable defineVariable(Location location, Class<?> type, String name, boolean isReadOnly) {
        if (isVariableDefined(name)) {
            throw location.createError(new IllegalArgumentException("variable [" + name + "] is already defined"));
        }

        Variable variable = new Variable(type, name, isReadOnly);
        variables.put(name, variable);

        return variable;
    }

    public abstract boolean isVariableDefined(String name);
    public abstract Variable getVariable(Location location, String name);

    public Variable defineInternalVariable(Location location, Class<?> type, String name, boolean isReadOnly) {
        return defineVariable(location, type, "#" + name, isReadOnly);
    }

    public Variable getInternalVariable(Location location, String name) {
        return getVariable(location, "#" + name);
    }
}

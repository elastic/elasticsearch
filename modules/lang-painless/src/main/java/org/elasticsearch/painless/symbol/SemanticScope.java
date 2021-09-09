/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.symbol.Decorator.Condition;
import org.elasticsearch.painless.symbol.Decorator.Decoration;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Tracks information within a scope required for compilation during the
 * semantic phase in the user tree. There are three types of scopes -
 * {@link FunctionScope}, {@link LambdaScope}, and {@link BlockScope}.
 *
 * Scopes are stacked as they are created during the user tree's semantic
 * phase with each scope beyond the top-level containing a reference to
 * its parent. As a scope is no longer necessary, it's dropped automatically
 * since parent scopes contain no references to child scopes.
 */
public abstract class SemanticScope {

    /**
     * Tracks information about both user-defined and internally-defined
     * variables. Each {@link SemanticScope} tracks its own set of defined
     * variables available for use.
     */
    public static class Variable {

        protected final Class<?> type;
        protected final String name;
        protected final boolean isFinal;

        public Variable(Class<?> type, String name, boolean isFinal) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
            this.isFinal = isFinal;
        }

        public Class<?> getType() {
            return type;
        }

        /**
         * Shortcut method to return this variable's canonical type name
         * often used in error messages.
         */
        public String getCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(type);
        }

        public String getName() {
            return name;
        }

        public boolean isFinal() {
            return isFinal;
        }
    }

    /**
     * Created whenever a new user-defined or internally-defined function
     * is generated. This is considered a top-level scope and has no parent.
     * This scope stores the return type for semantic validation of all
     * return statements within this function.
     */
    public static class FunctionScope extends SemanticScope {

        protected final Class<?> returnType;

        public FunctionScope(ScriptScope scriptScope, Class<?> returnType) {
            super(scriptScope, new HashSet<>());
            this.returnType = Objects.requireNonNull(returnType);
        }

        @Override
        public boolean isVariableDefined(String name) {
            return variables.containsKey(name);
        }

        /**
         * Returns the requested variable by name if found within this scope. Throws
         * an {@link IllegalArgumentException} if the variable is not found as this
         * is the top-level scope.
         */
        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                throw location.createError(new IllegalArgumentException("variable [" + name + "] is not defined"));
            }

            usedVariables.add(name);

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
    }

    /**
     * Created whenever a new user-defined lambda is generated. This scope
     * always has a parent scope used to search for possible captured variables
     * defined outside of this lambda's scope. Whenever a captured variable is
     * found, that variable is saved within this scope for signature generation
     * of the internally generated method for this lambda. This scope also
     * stores the return type for semantic validation of all return statements
     * within this lambda.
     */
    public static class LambdaScope extends SemanticScope {

        protected final SemanticScope parent;
        protected final Class<?> returnType;
        protected final Set<Variable> captures = new HashSet<>();
        protected boolean usesInstanceMethod = false;

        protected LambdaScope(SemanticScope parent, Class<?> returnType) {
            super(parent.scriptScope, parent.usedVariables);
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

        /**
         * Returns the requested variable by name if found within this scope.
         * Otherwise, requests the variable from the parent scope. Any variable
         * found within a parent scope is saved as a captured variable and returned
         * as final since captured variables cannot be modified within a lambda.
         */
        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                variable = parent.getVariable(location, name);
                variable = new Variable(variable.getType(), variable.getName(), true);
                captures.add(variable);
            } else {
                usedVariables.add(name);
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

        @Override
        public void setUsesInstanceMethod() {
            if (usesInstanceMethod) {
                return;
            }
            usesInstanceMethod = true;
        }

        @Override
        public boolean usesInstanceMethod() {
            return usesInstanceMethod;
        }
    }

    /**
     * Created whenever a new code block is generated such as an if statement or
     * a while loop. Stores information about variables defined within this scope.
     * Has a parent scope to search for variables defined in an outer scope as
     * necessary. Has no return type of its own as that is defined by either
     * a function or lambda, thus uses its parents return type when semantically
     * validating a return statement.
     */
    public static class BlockScope extends SemanticScope {

        protected final SemanticScope parent;

        protected BlockScope(SemanticScope parent) {
            super(parent.scriptScope, parent.usedVariables);
            this.parent = parent;
        }

        @Override
        public boolean isVariableDefined(String name) {
            if (variables.containsKey(name)) {
                return true;
            }

            return parent.isVariableDefined(name);
        }

        /**
         * Returns the requested variable by name if found within this scope.
         * Otherwise, requests the variable from the parent scope.
         */
        @Override
        public Variable getVariable(Location location, String name) {
            Objects.requireNonNull(location);
            Objects.requireNonNull(name);

            Variable variable = variables.get(name);

            if (variable == null) {
                variable = parent.getVariable(location, name);
            } else {
                usedVariables.add(name);
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

    /**
     * Returns a new function scope as the top-level scope with the
     * specified return type.
     */
    public static FunctionScope newFunctionScope(ScriptScope scriptScope, Class<?> returnType) {
        return new FunctionScope(scriptScope, returnType);
    }

    protected final ScriptScope scriptScope;

    protected final Map<String, Variable> variables = new HashMap<>();
    protected final Set<String> usedVariables;

    protected SemanticScope(ScriptScope scriptScope, Set<String> usedVariables) {
        this.scriptScope = Objects.requireNonNull(scriptScope);
        this.usedVariables = Objects.requireNonNull(usedVariables);
    }

    /**
     * Returns a new lambda scope with the current scope as
     * its parent and the specified return type.
     */
    public LambdaScope newLambdaScope(Class<?> returnType) {
        return new LambdaScope(this, returnType);
    }

    /**
     * Returns a new block scope with the current scope as
     * its parent.
     */
    public BlockScope newLocalScope() {
        return new BlockScope(this);
    }

    public ScriptScope getScriptScope() {
        return scriptScope;
    }

    public <T extends Decoration> T putDecoration(ANode node, T decoration) {
        return scriptScope.put(node.getIdentifier(), decoration);
    }

    public <T extends Decoration> T removeDecoration(ANode node, Class<T> type) {
        return scriptScope.remove(node.getIdentifier(), type);
    }

    public <T extends Decoration> T getDecoration(ANode node, Class<T> type) {
        return scriptScope.get(node.getIdentifier(), type);
    }

    public boolean hasDecoration(ANode node, Class<? extends Decoration> type) {
        return scriptScope.has(node.getIdentifier(), type);
    }

    public <T extends Decoration> boolean copyDecoration(ANode originalNode, ANode targetNode, Class<T> type) {
        return scriptScope.copy(originalNode.getIdentifier(), targetNode.getIdentifier(), type);
    }

    public boolean setCondition(ANode node, Class<? extends Condition> type) {
        return scriptScope.set(node.getIdentifier(), type);
    }

    public boolean deleteCondition(ANode node, Class<? extends Condition> type) {
        return scriptScope.delete(node.getIdentifier(), type);
    }

    public boolean getCondition(ANode node, Class<? extends Condition> type) {
        return scriptScope.exists(node.getIdentifier(), type);
    }

    public boolean replicateCondition(ANode originalNode, ANode targetNode, Class<? extends Condition> type) {
        return scriptScope.replicate(originalNode.getIdentifier(), targetNode.getIdentifier(), type);
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

    // We only want to track instance method use inside of lambdas for "this" injection.  It's a noop for other scopes.
    public void setUsesInstanceMethod() {}

    public boolean usesInstanceMethod() {
        return false;
    }

    public Variable defineInternalVariable(Location location, Class<?> type, String name, boolean isReadOnly) {
        return defineVariable(location, type, "#" + name, isReadOnly);
    }

    public boolean isInternalVariableDefined(String name) {
        return isVariableDefined("#" + name);
    }

    public Variable getInternalVariable(Location location, String name) {
        return getVariable(location, "#" + name);
    }

    /**
     * Returns the set of variables used within a top-level {@link FunctionScope}
     * including local variables no longer in scope upon completion of writing a
     * function to ASM bytecode.
     */
    public Set<String> getUsedVariables() {
        return Collections.unmodifiableSet(usedVariables);
    }
}

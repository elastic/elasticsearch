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
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;

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
        protected final boolean isGlobal;

        public Variable(Class<?> type, String name, boolean isFinal, boolean isGlobal) {
            this.type = Objects.requireNonNull(type);
            this.name = Objects.requireNonNull(name);
            this.isFinal = isFinal;
            this.isGlobal = isGlobal;
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

        public boolean isGlobal() {
            return isGlobal;
        }

        public boolean isFinal() {
            return isFinal;
        }
    }

    /**
     * Created when visiting a class.  Holds the global variables of the class.
     */
    public static class ClassScope extends SemanticScope {
        protected final Map<String, Variable> globals;
        public ClassScope(ScriptScope scriptScope) {
            super(scriptScope, new HashSet<>());
            this.globals = new HashMap<>();
        }

        @Override
        public boolean isVariableDefined(String name) {
            return globals.containsKey(name);
        }

        @Override
        public LocalFunction getLocalFunction() {
            throw new UnsupportedOperationException("ClassScope has no localFunction");
        }

        @Override
        public String getReturnCanonicalTypeName() {
            throw new UnsupportedOperationException("ClassScope has no returnType");
        }

        @Override
        public Variable getVariable(Location location, String name) {
            Variable global = globals.get(name);
            if (global != null) {
                return global;
            }
            throw location.createError(new IllegalArgumentException("variable [" + name + "] is not defined"));
        }

        @Override
        protected boolean isLocalVariableDefined(String name) {
            return false;
        }

        @Override
        protected Variable getLocalVariable(String name) {
            return null;
        }

        @Override
        protected boolean canDefineLocalVariable(String name) {
            return true;
        }

        @Override
        public Variable defineVariable(Location location, Class<?> type, String name, boolean isReadOnly) {
            if (isVariableDefined(name)) {
                throw location.createError(new IllegalArgumentException("variable [" + name + "] is already defined"));
            }

            Variable variable = new Variable(type, name, isReadOnly, true);
            globals.put(name, variable);

            return variable;
        }
    }

    /**
     * Created whenever a new user-defined or internally-defined function
     * is generated. This is considered a top-level scope and has no parent.
     * This scope stores the return type for semantic validation of all
     * return statements within this function.
     */
    public static class FunctionScope extends SemanticScope {

        protected final SemanticScope parent;
        protected final LocalFunction localFunction;
        protected final boolean isMainFunction;

        public FunctionScope(SemanticScope parent, LocalFunction localFunction, boolean isMainFunction) {
            super(parent.scriptScope, new HashSet<>());
            this.localFunction = Objects.requireNonNull(localFunction);
            this.parent = Objects.requireNonNull(parent);
            this.isMainFunction = isMainFunction;
        }

        @Override
        public boolean isVariableDefined(String name) {
            return variables.containsKey(name) || parent.isVariableDefined(name);
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
                if (parent.isVariableDefined(name) == false) {
                    throw location.createError(new IllegalArgumentException("variable [" + name + "] is not defined"));
                }
                variable = parent.getVariable(location, name);
                assert variable != null;
            }

            usedVariables.add(name);

            return variable;
        }

        @Override
        protected boolean isLocalVariableDefined(String name) {
            return variables.containsKey(name) || parent.isLocalVariableDefined(name);
        }

        @Override
        protected Variable getLocalVariable(String name) {
            Variable local = variables.get(name);
            if (local != null) {
                return local;
            }
            return parent.getLocalVariable(name);
        }

        @Override
        protected boolean canDefineLocalVariable(String name) {
            // Already defined
            if (isLocalVariableDefined(name)) {
                return false;
            }
            // Global
            if (isVariableDefined(name)) {
                // In execute
                return isMainFunction == false;
            }
            return true;
        }

        @Override
        public LocalFunction getLocalFunction() {
            return localFunction;
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(localFunction.returnType);
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
                if (parent.isVariableDefined(name) == false) {
                    throw location.createError(new IllegalArgumentException("variable [" + name + "] is not defined"));
                }
                variable = parent.getVariable(location, name);
                assert variable != null;

                variable = new Variable(variable.getType(), variable.getName(), true, variable.isGlobal());
                captures.add(variable);
            } else {
                usedVariables.add(name);
            }

            return variable;
        }

        @Override
        protected boolean isLocalVariableDefined(String name) {
            // TODO(stu): what should we do about captures here?
            return variables.containsKey(name) || parent.isLocalVariableDefined(name);
        }

        @Override
        protected Variable getLocalVariable(String name) {
            Variable local = variables.get(name);
            if (local != null) {
                return local;
            }
            return parent.getLocalVariable(name);
        }

        @Override
        protected boolean canDefineLocalVariable(String name) {
            return parent.canDefineLocalVariable(name);
        }

        @Override
        public Class<?> getReturnType() {
            return returnType;
        }

        @Override
        public LocalFunction getLocalFunction() {
            return parent.getLocalFunction();
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(parent.getLocalFunction().returnType);
        }

        public Set<Variable> getCaptures() {
            return Collections.unmodifiableSet(captures);
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
        protected boolean isLocalVariableDefined(String name) {
            return variables.containsKey(name) || parent.isLocalVariableDefined(name);
        }

        @Override
        protected Variable getLocalVariable(String name) {
            Variable local = variables.get(name);
            if (local != null) {
                return local;
            }
            return parent.getLocalVariable(name);
        }

        @Override
        protected boolean canDefineLocalVariable(String name) {
            if (isLocalVariableDefined(name)) {
                return false;
            }
            return parent.canDefineLocalVariable(name);
        }

        @Override
        public LocalFunction getLocalFunction() {
            return parent.getLocalFunction();
        }

        @Override
        public String getReturnCanonicalTypeName() {
            return parent.getReturnCanonicalTypeName();
        }
    }

    /**
     * Returns a new function scope with a parent ClassScope.
     */
    public static FunctionScope newFunctionScope(ClassScope classScope, LocalFunction localFunction) {
        return new FunctionScope(classScope, localFunction, false);
    }

    /**
     * Returns a new function scope for the execute function with a parent ClassScope.
     */
    public static FunctionScope newMainFunctionScope(ClassScope classScope, LocalFunction localFunction) {
        return new FunctionScope(classScope, localFunction, true);
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

    public abstract LocalFunction getLocalFunction();
    public abstract String getReturnCanonicalTypeName();

    /**
     * Can a local variable with the given name be defined?  A local variable with {@code name} cannot be defined if:
     *  - An existing local variable is already defined with name
     *  - A global variable is defined with name and we are in the main function
     *
     * Conversely, a local variable with {@code name} can be defined if:
     *  - No local variable exists with name
     *  - No global variable exists with name OR such a global exists and name is defined in the context of
     *    a user defined function.  In this case the local variable shadows the global variable.
     */
    protected abstract boolean canDefineLocalVariable(String name);

    public Class<?> getReturnType() {
        return getLocalFunction().returnType;
    }

    /**
     * Define a local variable at {@code location}, used for error messages if name already exists, of {@code type},
     * with {@code name}. {@code isReadyOnly} specifies if writes to this variable are rejected.
     */
    public Variable defineVariable(Location location, Class<?> type, String name, boolean isReadOnly) {
        if (canDefineLocalVariable(name) == false) {
            throw location.createError(new IllegalArgumentException("variable [" + name + "] is already defined"));
        }

        Variable variable = new Variable(type, name, isReadOnly, false);
        variables.put(name, variable);

        return variable;
    }

    /**
     * Is there any local or global variable defined with name?
     */
    public abstract boolean isVariableDefined(String name);

    /**
     * Fetches the variable or throws an IllegalArgumentException if the variable is not defined
     * @param location location of the error, if variable not defined
     * @param name name of the variable
     * @return Variable, if defined
     * @throws IllegalArgumentException if variable is not defined
     */
    public abstract Variable getVariable(Location location, String name);

    /**
     * Is there a local variable defined with name?
     */
    protected abstract boolean isLocalVariableDefined(String name);

    /**
     * Gets the local variable with name, or {@code null} if no such variable is defined.
     */
    protected abstract Variable getLocalVariable(String name);

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

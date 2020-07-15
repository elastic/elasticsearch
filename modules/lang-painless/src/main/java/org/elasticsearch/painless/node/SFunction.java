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
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope.FunctionScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.symbol.SemanticScope.newFunctionScope;

/**
 * Represents a user-defined function.
 */
public class SFunction extends ANode {

    private final String returnCanonicalTypeName;
    private final String functionName;
    private final List<String> canonicalTypeNameParameters;
    private final List<String> parameterNames;
    private final SBlock blockNode;
    private final boolean isInternal;
    private final boolean isStatic;
    private final boolean isSynthetic;
    private final boolean isAutoReturnEnabled;

    public SFunction(int identifier, Location location,
            String returnCanonicalTypeName, String name, List<String> canonicalTypeNameParameters, List<String> parameterNames,
            SBlock blockNode,
            boolean isInternal, boolean isStatic, boolean isSynthetic, boolean isAutoReturnEnabled) {

        super(identifier, location);

        this.returnCanonicalTypeName = Objects.requireNonNull(returnCanonicalTypeName);
        this.functionName = Objects.requireNonNull(name);
        this.canonicalTypeNameParameters = Collections.unmodifiableList(Objects.requireNonNull(canonicalTypeNameParameters));
        this.parameterNames = Collections.unmodifiableList(Objects.requireNonNull(parameterNames));
        this.blockNode = Objects.requireNonNull(blockNode);
        this.isInternal = isInternal;
        this.isSynthetic = isSynthetic;
        this.isStatic = isStatic;
        this.isAutoReturnEnabled = isAutoReturnEnabled;
    }

    public String getReturnCanonicalTypeName() {
        return returnCanonicalTypeName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public List<String> getCanonicalTypeNameParameters() {
        return canonicalTypeNameParameters;
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    public boolean isInternal() {
        return isInternal;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public boolean isSynthetic() {
        return isSynthetic;
    }

    /**
     * If set to {@code true} default return values are inserted if
     * not all paths return a value.
     */
    public boolean isAutoReturnEnabled() {
        return isAutoReturnEnabled;
    }

    @Override
    public <Input, Output> Output visit(UserTreeVisitor<Input, Output> userTreeVisitor, Input input) {
        return userTreeVisitor.visitFunction(this, input);
    }

    void buildClassScope(ScriptScope scriptScope) {
        if (canonicalTypeNameParameters.size() != parameterNames.size()) {
            throw createError(new IllegalStateException(
                "parameter types size [" + canonicalTypeNameParameters.size() + "] is not equal to " +
                "parameter names size [" + parameterNames.size() + "]"));
        }

        PainlessLookup painlessLookup = scriptScope.getPainlessLookup();
        FunctionTable functionTable = scriptScope.getFunctionTable();

        String functionKey = FunctionTable.buildLocalFunctionKey(functionName, canonicalTypeNameParameters.size());

        if (functionTable.getFunction(functionKey) != null) {
            throw createError(new IllegalArgumentException("illegal duplicate functions [" + functionKey + "]."));
        }

        Class<?> returnType = painlessLookup.canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw createError(new IllegalArgumentException(
                "return type [" + returnCanonicalTypeName + "] not found for function [" + functionKey + "]"));
        }

        List<Class<?>> typeParameters = new ArrayList<>();

        for (String typeParameter : canonicalTypeNameParameters) {
            Class<?> paramType = painlessLookup.canonicalTypeNameToType(typeParameter);

            if (paramType == null) {
                throw createError(new IllegalArgumentException(
                    "parameter type [" + typeParameter + "] not found for function [" + functionKey + "]"));
            }

            typeParameters.add(paramType);
        }

        functionTable.addFunction(functionName, returnType, typeParameters, isInternal, isStatic);
    }

    void analyze(ScriptScope scriptScope) {
        FunctionTable.LocalFunction localFunction =
                scriptScope.getFunctionTable().getFunction(functionName, canonicalTypeNameParameters.size());
        Class<?> returnType = localFunction.getReturnType();
        List<Class<?>> typeParameters = localFunction.getTypeParameters();
        FunctionScope functionScope = newFunctionScope(scriptScope, localFunction.getReturnType());

        for (int index = 0; index < localFunction.getTypeParameters().size(); ++index) {
            Class<?> typeParameter = localFunction.getTypeParameters().get(index);
            String parameterName = parameterNames.get(index);
            functionScope.defineVariable(getLocation(), typeParameter, parameterName, false);
        }

        if (blockNode.getStatementNodes().isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty function [" + functionName + "]."));
        }

        functionScope.setCondition(blockNode, LastSource.class);
        blockNode.analyze(functionScope.newLocalScope());
        boolean methodEscape = functionScope.getCondition(blockNode, MethodEscape.class);

        if (methodEscape == false && isAutoReturnEnabled == false && returnType != void.class) {
            throw createError(new IllegalArgumentException("not all paths provide a return value " +
                    "for function [" + functionName + "] with [" + typeParameters.size() + "] parameters"));
        }

        if (methodEscape) {
            functionScope.setCondition(this, MethodEscape.class);
        }

        // TODO: do not specialize for execute
        // TODO: https://github.com/elastic/elasticsearch/issues/51841
        if ("execute".equals(functionName)) {
            scriptScope.setUsedVariables(functionScope.getUsedVariables());
        }
        // TODO: end
    }
}

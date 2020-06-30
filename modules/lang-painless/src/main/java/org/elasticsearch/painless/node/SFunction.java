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
import org.elasticsearch.painless.symbol.SemanticScope.FunctionScope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.node.AStatement.Input;
import org.elasticsearch.painless.node.AStatement.Output;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptScope;

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

    FunctionNode analyze(ClassNode classNode, ScriptScope scriptScope) {
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

        int maxLoopCounter = scriptScope.getCompilerSettings().getMaxLoopCounter();

        if (blockNode.getStatementNodes().isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty function [" + functionName + "]."));
        }

        Input blockInput = new Input();
        blockInput.lastSource = true;
        Output blockOutput = blockNode.analyze(classNode, functionScope.newLocalScope(), blockInput);
        boolean methodEscape = blockOutput.methodEscape;

        if (methodEscape == false && isAutoReturnEnabled == false && returnType != void.class) {
            throw createError(new IllegalArgumentException("not all paths provide a return value " +
                    "for function [" + functionName + "] with [" + typeParameters.size() + "] parameters"));
        }

        // TODO: do not specialize for execute
        // TODO: https://github.com/elastic/elasticsearch/issues/51841
        if ("execute".equals(functionName)) {
            scriptScope.setUsedVariables(functionScope.getUsedVariables());
        }
        // TODO: end

        BlockNode blockNode = (BlockNode)blockOutput.statementNode;

        if (methodEscape == false) {
            ExpressionNode expressionNode;

            if (returnType == void.class) {
                expressionNode = null;
            } else if (isAutoReturnEnabled) {
                if (returnType.isPrimitive()) {
                    ConstantNode constantNode = new ConstantNode();
                    constantNode.setLocation(getLocation());
                    constantNode.setExpressionType(returnType);

                    if (returnType == boolean.class) {
                        constantNode.setConstant(false);
                    } else if (returnType == byte.class
                            || returnType == char.class
                            || returnType == short.class
                            || returnType == int.class) {
                        constantNode.setConstant(0);
                    } else if (returnType == long.class) {
                        constantNode.setConstant(0L);
                    } else if (returnType == float.class) {
                        constantNode.setConstant(0f);
                    } else if (returnType == double.class) {
                        constantNode.setConstant(0d);
                    } else {
                        throw createError(new IllegalStateException("unexpected automatic return type " +
                                "[" + PainlessLookupUtility.typeToCanonicalTypeName(returnType) + "] " +
                                "for function [" + functionName + "] with [" + typeParameters.size() + "] parameters"));
                    }

                    expressionNode = constantNode;
                } else {
                    expressionNode = new NullNode();
                    expressionNode.setLocation(getLocation());
                    expressionNode.setExpressionType(returnType);
                }
            } else {
                throw createError(new IllegalStateException("not all paths provide a return value " +
                        "for function [" + functionName + "] with [" + typeParameters.size() + "] parameters"));
            }

            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(getLocation());
            returnNode.setExpressionNode(expressionNode);

            blockNode.addStatementNode(returnNode);
        }

        FunctionNode functionNode = new FunctionNode();

        functionNode.setBlockNode(blockNode);

        functionNode.setLocation(getLocation());
        functionNode.setName(functionName);
        functionNode.setReturnType(returnType);
        functionNode.getTypeParameters().addAll(typeParameters);
        functionNode.getParameterNames().addAll(parameterNames);
        functionNode.setStatic(isStatic);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(isSynthetic);
        functionNode.setMaxLoopCounter(maxLoopCounter);

        return functionNode;
    }
}

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
import org.elasticsearch.painless.Scope.FunctionScope;
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
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.painless.Scope.newFunctionScope;

/**
 * Represents a user-defined function.
 */
public class SFunction extends ANode {

    protected final String rtnTypeStr;
    protected final String name;
    protected final List<String> paramTypeStrs;
    protected final List<String> paramNameStrs;
    protected final SBlock block;
    protected final boolean isInternal;
    protected final boolean isStatic;
    protected final boolean synthetic;

    /**
     * If set to {@code true} default return values are inserted if
     * not all paths return a value.
     */
    protected final boolean isAutoReturnEnabled;

    public SFunction(Location location, String rtnType, String name,
            List<String> paramTypes, List<String> paramNames,
            SBlock block, boolean isInternal, boolean isStatic, boolean synthetic, boolean isAutoReturnEnabled) {
        super(location);

        this.rtnTypeStr = Objects.requireNonNull(rtnType);
        this.name = Objects.requireNonNull(name);
        this.paramTypeStrs = Collections.unmodifiableList(paramTypes);
        this.paramNameStrs = Collections.unmodifiableList(paramNames);
        this.block = Objects.requireNonNull(block);
        this.isInternal = isInternal;
        this.synthetic = synthetic;
        this.isStatic = isStatic;
        this.isAutoReturnEnabled = isAutoReturnEnabled;
    }

    void buildClassScope(ScriptRoot scriptRoot) {
        if (paramTypeStrs.size() != paramNameStrs.size()) {
            throw createError(new IllegalStateException(
                "parameter types size [" + paramTypeStrs.size() + "] is not equal to " +
                "parameter names size [" + paramNameStrs.size() + "]"));
        }

        PainlessLookup painlessLookup = scriptRoot.getPainlessLookup();
        FunctionTable functionTable = scriptRoot.getFunctionTable();

        String functionKey = FunctionTable.buildLocalFunctionKey(name, paramTypeStrs.size());

        if (functionTable.getFunction(functionKey) != null) {
            throw createError(new IllegalArgumentException("illegal duplicate functions [" + functionKey + "]."));
        }

        Class<?> returnType = painlessLookup.canonicalTypeNameToType(rtnTypeStr);

        if (returnType == null) {
            throw createError(new IllegalArgumentException(
                "return type [" + rtnTypeStr + "] not found for function [" + functionKey + "]"));
        }

        List<Class<?>> typeParameters = new ArrayList<>();

        for (String typeParameter : paramTypeStrs) {
            Class<?> paramType = painlessLookup.canonicalTypeNameToType(typeParameter);

            if (paramType == null) {
                throw createError(new IllegalArgumentException(
                    "parameter type [" + typeParameter + "] not found for function [" + functionKey + "]"));
            }

            typeParameters.add(paramType);
        }

        functionTable.addFunction(name, returnType, typeParameters, isInternal, isStatic);
    }

    FunctionNode writeFunction(ClassNode classNode, ScriptRoot scriptRoot) {
        FunctionTable.LocalFunction localFunction = scriptRoot.getFunctionTable().getFunction(name, paramTypeStrs.size());
        Class<?> returnType = localFunction.getReturnType();
        List<Class<?>> typeParameters = localFunction.getTypeParameters();
        FunctionScope functionScope = newFunctionScope(localFunction.getReturnType());

        for (int index = 0; index < localFunction.getTypeParameters().size(); ++index) {
            Class<?> typeParameter = localFunction.getTypeParameters().get(index);
            String parameterName = paramNameStrs.get(index);
            functionScope.defineVariable(location, typeParameter, parameterName, false);
        }

        int maxLoopCounter = scriptRoot.getCompilerSettings().getMaxLoopCounter();

        if (block.statements.isEmpty()) {
            throw createError(new IllegalArgumentException("Cannot generate an empty function [" + name + "]."));
        }

        Input blockInput = new Input();
        blockInput.lastSource = true;
        Output blockOutput = block.analyze(classNode, scriptRoot, functionScope.newLocalScope(), blockInput);
        boolean methodEscape = blockOutput.methodEscape;

        if (methodEscape == false && isAutoReturnEnabled == false && returnType != void.class) {
            throw createError(new IllegalArgumentException("not all paths provide a return value " +
                    "for function [" + name + "] with [" + typeParameters.size() + "] parameters"));
        }

        // TODO: do not specialize for execute
        // TODO: https://github.com/elastic/elasticsearch/issues/51841
        if ("execute".equals(name)) {
            scriptRoot.setUsedVariables(functionScope.getUsedVariables());
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
                    constantNode.setLocation(location);
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
                                "for function [" + name + "] with [" + typeParameters.size() + "] parameters"));
                    }

                    expressionNode = constantNode;
                } else {
                    expressionNode = new NullNode();
                    expressionNode.setLocation(location);
                    expressionNode.setExpressionType(returnType);
                }
            } else {
                throw createError(new IllegalStateException("not all paths provide a return value " +
                        "for function [" + name + "] with [" + typeParameters.size() + "] parameters"));
            }

            ReturnNode returnNode = new ReturnNode();
            returnNode.setLocation(location);
            returnNode.setExpressionNode(expressionNode);

            blockNode.addStatementNode(returnNode);
        }

        FunctionNode functionNode = new FunctionNode();

        functionNode.setBlockNode(blockNode);

        functionNode.setLocation(location);
        functionNode.setName(name);
        functionNode.setReturnType(returnType);
        functionNode.getTypeParameters().addAll(typeParameters);
        functionNode.getParameterNames().addAll(paramNameStrs);
        functionNode.setStatic(isStatic);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(synthetic);
        functionNode.setMaxLoopCounter(maxLoopCounter);

        return functionNode;
    }
}

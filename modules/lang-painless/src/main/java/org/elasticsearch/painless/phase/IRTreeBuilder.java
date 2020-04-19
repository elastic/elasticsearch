/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;

public class IRTreeBuilder implements UserTreeVisitor<ScriptScope, IRNode> {

    private static final Location INTERNAL_LOCATION = new Location("<internal>", 0);

    private ClassNode irClassNode;

    public IRNode visit(ANode userNode, ScriptScope scriptScope) {
        return userNode.visit(this, scriptScope);
    }

    @Override
    public ClassNode visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode();

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

        irClassNode.setLocation(irClassNode.getLocation());
        irClassNode.setScriptScope(scriptScope);

        return irClassNode;
    }

    @Override
    public FunctionNode visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        int functionArity = userFunctionNode.getCanonicalTypeNameParameters().size();
        LocalFunction localFunction = scriptScope.getFunctionTable().getFunction(functionName, functionArity);
        Class<?> returnType = localFunction.getReturnType();
        boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);

        BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

        if (methodEscape == false) {
            ExpressionNode irExpressionNode;

            if (returnType == void.class) {
                irExpressionNode = null;
            } else if (userFunctionNode.isAutoReturnEnabled()) {
                if (returnType.isPrimitive()) {
                    ConstantNode constantNode = new ConstantNode();
                    constantNode.setLocation(INTERNAL_LOCATION);
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
                        throw userFunctionNode.createError(new IllegalStateException("unexpected automatic return type " +
                                "[" + PainlessLookupUtility.typeToCanonicalTypeName(returnType) + "] " +
                                "for function [" + functionName + "] with [" + functionArity + "] parameters"));
                    }

                    irExpressionNode = constantNode;
                } else {
                    irExpressionNode = new NullNode();
                    irExpressionNode.setLocation(INTERNAL_LOCATION);
                    irExpressionNode.setExpressionType(returnType);
                }
            } else {
                throw userFunctionNode.createError(new IllegalStateException("not all paths provide a return value " +
                        "for function [" + functionName + "] with [" + functionArity + "] parameters"));
            }

            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setLocation(INTERNAL_LOCATION);
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setBlockNode(irBlockNode);
        irFunctionNode.setLocation(userFunctionNode.getLocation());
        irFunctionNode.setName(userFunctionNode.getFunctionName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
        irFunctionNode.getParameterNames().addAll(userFunctionNode.getParameterNames());
        irFunctionNode.setStatic(userFunctionNode.isStatic());
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(userFunctionNode.isSynthetic());
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

        return irFunctionNode;
    }

    @Override
    public BlockNode visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode();

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setLocation(userBlockNode.getLocation());
        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        return irBlockNode;
    }
}

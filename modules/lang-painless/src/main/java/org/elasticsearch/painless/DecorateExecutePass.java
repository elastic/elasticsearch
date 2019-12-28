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

import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.UnboundCallNode;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptRoot;
import org.objectweb.asm.commons.Method;

import java.util.Collections;

public class DecorateExecutePass {

    public static void pass(ScriptRoot scriptRoot, ClassNode classNode) {
        FunctionNode executeFunctionNode = null;

        for (FunctionNode functionNode : classNode.getFunctionsNodes()) {
            if ("execute".equals(functionNode.getName())) {
                executeFunctionNode = functionNode;
                break;
            }
        }

        BlockNode blockNode = executeFunctionNode.getBlockNode();

        // convert gets methods to a new set of inserted ir nodes as necessary -
        // requires the gets method name be modified from "getExample" to "example"
        // if a get method variable isn't used it's declaration node is removed from
        // the ir tree permanently so there is no frivolous variable slotting
        int statementIndex = 0;

        while (statementIndex < blockNode.getStatementsNodes().size()) {
            StatementNode statementNode = blockNode.getStatementsNodes().get(statementIndex);

            if (statementNode instanceof DeclarationNode) {
                DeclarationNode declarationNode = (DeclarationNode) statementNode;
                boolean isRemoved = false;

                for (int getIndex = 0; getIndex < scriptRoot.getScriptClassInfo().getGetMethods().size(); ++getIndex) {
                    Class<?> returnType = scriptRoot.getScriptClassInfo().getGetReturns().get(getIndex);
                    Method getMethod = scriptRoot.getScriptClassInfo().getGetMethods().get(getIndex);
                    String name = getMethod.getName().substring(3);
                    name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

                    if (name.equals(declarationNode.getName())) {
                        if (scriptRoot.getUsedVariables().contains(name)) {
                                UnboundCallNode unboundCallNode = new UnboundCallNode();
                                unboundCallNode.setLocation(declarationNode.getLocation());
                                unboundCallNode.setExpressionType(declarationNode.getDeclarationType());
                                unboundCallNode.setLocalFunction(new LocalFunction(
                                        getMethod.getName(), returnType, Collections.emptyList(), true, false));
                                declarationNode.setExpressionNode(unboundCallNode);
                        } else {
                            blockNode.getStatementsNodes().remove(statementIndex);
                            isRemoved = true;
                        }

                        break;
                    }
                }

                if (isRemoved == false) {
                    ++statementIndex;
                }
            } else {
                ++statementIndex;
            }
        }

        /*blockNode = new BlockNode()
                .addStatementNode(new TryNode())
                .setLocation(blockNode.getLocation())
                .setAllEscape(blockNode.doAllEscape())
                .setStatementCount(blockNode.getStatementCount());*/
    }

    protected DecorateExecutePass() {
        // do nothing
    }
}

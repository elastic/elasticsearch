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

import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope.FunctionScope;

import java.util.List;

import static org.elasticsearch.painless.symbol.SemanticScope.newFunctionScope;

public class PainlessSemanticAnalysisPhase extends DefaultSemanticAnalysisPhase {

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();

        if ("execute".equals(functionName)) {
            ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();
            LocalFunction localFunction =
                    scriptScope.getFunctionTable().getFunction(functionName, scriptClassInfo.getExecuteArguments().size());
            List<Class<?>> typeParameters = localFunction.getTypeParameters();
            FunctionScope functionScope = newFunctionScope(scriptScope, localFunction.getReturnType());

            for (int i = 0; i < typeParameters.size(); ++i) {
                Class<?> typeParameter = localFunction.getTypeParameters().get(i);
                String parameterName = scriptClassInfo.getExecuteArguments().get(i).getName();
                functionScope.defineVariable(userFunctionNode.getLocation(), typeParameter, parameterName, false);
            }

            for (int i = 0; i < scriptClassInfo.getGetMethods().size(); ++i) {
                Class<?> typeParameter = scriptClassInfo.getGetReturns().get(i);
                org.objectweb.asm.commons.Method method = scriptClassInfo.getGetMethods().get(i);
                String parameterName = method.getName().substring(3);
                parameterName = Character.toLowerCase(parameterName.charAt(0)) + parameterName.substring(1);
                functionScope.defineVariable(userFunctionNode.getLocation(), typeParameter, parameterName, false);
            }

            SBlock userBlockNode = userFunctionNode.getBlockNode();

            if (userBlockNode.getStatementNodes().isEmpty()) {
                throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                        "found no statements for function " +
                        "[" + functionName + "] with [" + typeParameters.size() + "] parameters"));
            }

            functionScope.setCondition(userBlockNode, LastSource.class);
            visit(userBlockNode, functionScope.newLocalScope());
            boolean methodEscape = functionScope.getCondition(userBlockNode, MethodEscape.class);

            if (methodEscape) {
                functionScope.setCondition(userFunctionNode, MethodEscape.class);
            }

            scriptScope.setUsedVariables(functionScope.getUsedVariables());
        } else {
            super.visitFunction(userFunctionNode, scriptScope);
        }
    }
}

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
import org.elasticsearch.painless.ScriptClassInfo.MethodArgument;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.ArrayList;
import java.util.List;

public class PainlessSemanticHeaderPhase extends DefaultSemanticHeaderPhase {

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();

        if ("execute".equals(functionName)) {
            ScriptClassInfo scriptClassInfo = scriptScope.getScriptClassInfo();

            FunctionTable functionTable = scriptScope.getFunctionTable();
            String functionKey = FunctionTable.buildLocalFunctionKey(functionName, scriptClassInfo.getExecuteArguments().size());

            if (functionTable.getFunction(functionKey) != null) {
                throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                        "found duplicate function [" + functionKey + "]."));
            }

            Class<?> returnType = scriptClassInfo.getExecuteMethodReturnType();
            List<Class<?>> typeParameters = new ArrayList<>();

            for (MethodArgument methodArgument : scriptClassInfo.getExecuteArguments()) {
                typeParameters.add(methodArgument.getClazz());
            }

            functionTable.addFunction(functionName, returnType, typeParameters, true, false);
        } else {
            super.visitFunction(userFunctionNode, scriptScope);
        }
    }
}

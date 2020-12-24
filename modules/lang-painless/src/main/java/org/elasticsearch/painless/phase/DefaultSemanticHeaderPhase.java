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

import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.ArrayList;
import java.util.List;

public class DefaultSemanticHeaderPhase extends UserTreeBaseVisitor<ScriptScope> {

    @Override
    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            visitFunction(userFunctionNode, scriptScope);
        }
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        List<String> canonicalTypeNameParameters = userFunctionNode.getCanonicalTypeNameParameters();
        List<String> parameterNames = userFunctionNode.getParameterNames();
        int parameterCount = canonicalTypeNameParameters.size();

        if (parameterCount != parameterNames.size()) {
            throw userFunctionNode.createError(new IllegalStateException("invalid function definition: " +
                    "parameter types size [" + canonicalTypeNameParameters.size() + "] is not equal to " +
                    "parameter names size [" + parameterNames.size() + "] for function [" + functionName +"]"));
        }

        FunctionTable functionTable = scriptScope.getFunctionTable();
        String functionKey = FunctionTable.buildLocalFunctionKey(functionName, canonicalTypeNameParameters.size());

        if (functionTable.getFunction(functionKey) != null) {
            throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                    "found duplicate function [" + functionKey + "]."));
        }

        PainlessLookup painlessLookup = scriptScope.getPainlessLookup();
        String returnCanonicalTypeName = userFunctionNode.getReturnCanonicalTypeName();
        Class<?> returnType = painlessLookup.canonicalTypeNameToType(returnCanonicalTypeName);

        if (returnType == null) {
            throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                    "return type [" + returnCanonicalTypeName + "] not found for function [" + functionKey + "]"));
        }

        List<Class<?>> typeParameters = new ArrayList<>();

        for (String typeParameter : canonicalTypeNameParameters) {
            Class<?> paramType = painlessLookup.canonicalTypeNameToType(typeParameter);

            if (paramType == null) {
                throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                        "parameter type [" + typeParameter + "] not found for function [" + functionKey + "]"));
            }

            typeParameters.add(paramType);
        }

        functionTable.addFunction(functionName, returnType, typeParameters, userFunctionNode.isInternal(), userFunctionNode.isStatic());
    }
}

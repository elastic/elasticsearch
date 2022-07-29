/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
                throw userFunctionNode.createError(
                    new IllegalArgumentException("invalid function definition: " + "found duplicate function [" + functionKey + "].")
                );
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

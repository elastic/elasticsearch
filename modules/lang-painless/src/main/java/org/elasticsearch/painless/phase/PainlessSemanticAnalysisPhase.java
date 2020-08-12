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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.FunctionScope;

import java.lang.reflect.Method;
import java.util.List;

import static org.elasticsearch.painless.symbol.SemanticScope.newFunctionScope;

public class PainlessSemanticAnalysisPhase extends DefaultSemanticAnalysisPhase {

    /** Current function while in {@code visitFunction} */
    protected String functionName = "";

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        functionName = userFunctionNode.getFunctionName();

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

        functionName = "";
    }

    /**
     * Visits an expression that is also considered a statement.
     *
     * If the statement is a return from the execute method, performs return value conversion.
     *
     * Checks: control flow, type validation
     */
    @Override
    public void visitExpression(SExpression userExpressionNode, SemanticScope semanticScope) {
        Class<?> rtnType = semanticScope.getReturnType();
        boolean isVoid = rtnType == void.class;
        boolean lastSource = semanticScope.getCondition(userExpressionNode, LastSource.class);
        AExpression userStatementNode = userExpressionNode.getStatementNode();

        if (lastSource && isVoid == false) {
            semanticScope.setCondition(userStatementNode, Read.class);
        }

        checkedVisit(userStatementNode, semanticScope);
        Class<?> expressionValueType = semanticScope.getDecoration(userStatementNode, Decorations.ValueType.class).getValueType();
        boolean rtn = lastSource && isVoid == false && expressionValueType != void.class;

        if (rtn) {
            semanticScope.putDecoration(userStatementNode, new TargetType(rtnType));
            semanticScope.setCondition(userStatementNode, Internal.class);
            if ("execute".equals(functionName)) {
                decorateWithCast(userStatementNode, semanticScope, semanticScope.getScriptScope().getScriptClassInfo());
            } else {
                decorateWithCast(userStatementNode, semanticScope);
            }

            semanticScope.setCondition(userExpressionNode, MethodEscape.class);
            semanticScope.setCondition(userExpressionNode, LoopEscape.class);
            semanticScope.setCondition(userExpressionNode, AllEscape.class);
        }
    }

    /**
     * Visits a return statement and casts the value to the return type if possible.
     *
     * If the statement is a return from the execute method, performs return value conversion.
     *
     * Checks: type validation
     */
    @Override
    public void visitReturn(SReturn userReturnNode, SemanticScope semanticScope) {
        AExpression userValueNode = userReturnNode.getValueNode();

        if (userValueNode == null) {
            if (semanticScope.getReturnType() != void.class) {
                throw userReturnNode.createError(new ClassCastException("cannot cast from " +
                    "[" + semanticScope.getReturnCanonicalTypeName() + "] to " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]"));
            }
        } else {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(semanticScope.getReturnType()));
            semanticScope.setCondition(userValueNode, Internal.class);
            checkedVisit(userValueNode, semanticScope);
            if ("execute".equals(functionName)) {
                decorateWithCast(userValueNode, semanticScope, semanticScope.getScriptScope().getScriptClassInfo());
            } else {
                decorateWithCast(userValueNode, semanticScope);
            }
        }

        semanticScope.setCondition(userReturnNode, MethodEscape.class);
        semanticScope.setCondition(userReturnNode, LoopEscape.class);
        semanticScope.setCondition(userReturnNode, AllEscape.class);
    }

    /**
     * Decorates a user expression node with a PainlessCast.
     */
    public void decorateWithCast(AExpression userExpressionNode, SemanticScope semanticScope, ScriptClassInfo scriptClassInfo) {
        Location location = userExpressionNode.getLocation();
        Class<?> valueType = semanticScope.getDecoration(userExpressionNode, Decorations.ValueType.class).getValueType();
        Class<?> targetType = semanticScope.getDecoration(userExpressionNode, TargetType.class).getTargetType();
        boolean isExplicitCast = semanticScope.getCondition(userExpressionNode, Decorations.Explicit.class);
        boolean isInternalCast = semanticScope.getCondition(userExpressionNode, Internal.class);

        PainlessCast painlessCast;
        Method converter = scriptClassInfo.getConverter(valueType);
        if (converter != null) {
            painlessCast = PainlessCast.convertedReturn(valueType, targetType, converter);
        } else {
            painlessCast = AnalyzerCaster.getLegalCast(location, valueType, targetType, isExplicitCast, isInternalCast);
        }

        if (painlessCast != null) {
            semanticScope.putDecoration(userExpressionNode, new ExpressionPainlessCast(painlessCast));
        }
    }
}

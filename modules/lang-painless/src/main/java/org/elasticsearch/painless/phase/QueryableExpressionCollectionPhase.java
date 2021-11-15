/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.symbol.IRDecorations;
import org.elasticsearch.painless.symbol.QueryableExpressionScope;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

import java.lang.reflect.Method;

/**
 * Constructs the QueryableExpression if possible.
 */
public class QueryableExpressionCollectionPhase extends IRTreeBaseVisitor<QueryableExpressionScope> {

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, QueryableExpressionScope queryableExpressionScope) {
        super.visitInvokeCall(irInvokeCallNode, queryableExpressionScope);
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, QueryableExpressionScope queryableExpressionScope) {
        IRDecorations.IRDClassBinding irdBinding = irInvokeCallMemberNode.getDecoration(IRDecorations.IRDClassBinding.class);
        if (irdBinding != null) {
            Method method = irdBinding.getValue().javaMethod;
            if (method.getDeclaringClass().getName().endsWith("$Emit") && method.getName().equals("emit")) {
                // TODO: handle emit
            }
        }

        super.visitInvokeCallMember(irInvokeCallMemberNode, queryableExpressionScope);
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, QueryableExpressionScope queryableExpressionScope) {
        IRDecorations.IRDValue rightNodeValue = irBinaryImplNode.getRightNode().getDecoration(IRDecorations.IRDValue.class);
        if (rightNodeValue != null && rightNodeValue.getValue().equals("value")) {
            // TODO: get accessed field from lhs
        }

        super.visitBinaryImpl(irBinaryImplNode, queryableExpressionScope);
    }

    @Override
    public void visitConstant(ConstantNode irConstantNode, QueryableExpressionScope scope) {
        Object value = irConstantNode.getDecorationValue(IRDecorations.IRDConstant.class);
        scope.push(QueryableExpressionBuilder.constant(value));
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, QueryableExpressionScope scope) {
        super.visitBinaryMath(irBinaryMathNode, scope);

        Operation operation = irBinaryMathNode.getDecorationValue(IRDecorations.IRDOperation.class);

        scope.consume((lhs, rhs) -> {
            if (operation == Operation.ADD) {
                return QueryableExpressionBuilder.add(lhs, rhs);
            } else if (operation == Operation.SUB) {
                return QueryableExpressionBuilder.subtract(lhs, rhs);
            } else if (operation == Operation.MUL) {
                return QueryableExpressionBuilder.multiply(lhs, rhs);
            } else if (operation == Operation.DIV) {
                return QueryableExpressionBuilder.divide(lhs, rhs);
            } else {
                return QueryableExpressionBuilder.UNQUERYABLE;
            }
        });
    }
}

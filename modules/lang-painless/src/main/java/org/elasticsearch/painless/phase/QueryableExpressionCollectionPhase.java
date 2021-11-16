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
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.LoadDotDefNode;
import org.elasticsearch.painless.ir.LoadVariableNode;
import org.elasticsearch.painless.symbol.IRDecorations;
import org.elasticsearch.painless.symbol.QueryableExpressionScope;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

import java.lang.reflect.Method;

/**
 * Constructs the QueryableExpression if possible.
 */
public class QueryableExpressionCollectionPhase extends IRTreeBaseVisitor<QueryableExpressionScope> {

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, QueryableExpressionScope scope) {
        super.visitInvokeCall(irInvokeCallNode, scope);
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, QueryableExpressionScope scope) {
        IRDecorations.IRDClassBinding irdBinding = irInvokeCallMemberNode.getDecoration(IRDecorations.IRDClassBinding.class);

        if (irdBinding != null) {
            Method method = irdBinding.getValue().javaMethod;
            if (method.getDeclaringClass().getName().endsWith("$Emit") && method.getName().equals("emit")) {
                scope.enterEmit();
                super.visitInvokeCallMember(irInvokeCallMemberNode, scope);
                scope.exitEmit();
                return;
            }
        }

        super.visitInvokeCallMember(irInvokeCallMemberNode, scope);
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, QueryableExpressionScope scope) {
        if (irBinaryImplNode.getLeftNode() instanceof BinaryImplNode) {
            ConstantNode docLookupKeyNode = lookupKeyForMapAccessOnVariable("doc", (BinaryImplNode) irBinaryImplNode.getLeftNode());
            if (docLookupKeyNode != null && rightChildIsValueAccess(irBinaryImplNode)) {
                Object value = docLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
                if (value instanceof String) {
                    scope.push(QueryableExpressionBuilder.field((String) value));
                    return;
                }
            }
        }

        ConstantNode paramsLookupKeyNode = lookupKeyForMapAccessOnVariable("params", irBinaryImplNode);
        if (paramsLookupKeyNode != null) {
            Object value = paramsLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
            if (value instanceof String) {
                scope.push(QueryableExpressionBuilder.param((String) value));
                return;
            }
        }
    }

    /**
     * Checks whether a BinaryImplNode references a variable `variableName` on which a Map lookup is performed.
     * Returns the lookup key if it is a constant.
     */
    private ConstantNode lookupKeyForMapAccessOnVariable(String variableName, BinaryImplNode irBinaryImplNode) {
        if (irBinaryImplNode.getLeftNode() instanceof LoadVariableNode) {
            // <variableName>.get('<field>') syntax
            IRDecorations.IRDName name = irBinaryImplNode.getLeftNode().getDecoration(IRDecorations.IRDName.class);
            if (name != null && name.getValue().equals(variableName) && irBinaryImplNode.getRightNode() instanceof InvokeCallNode) {
                InvokeCallNode invokeCall = (InvokeCallNode) irBinaryImplNode.getRightNode();
                if (invokeCall.getMethod().javaMethod.getName().equals("get") && invokeCall.getArgumentNodes().size() == 1) {
                    return constantNodeOrNull(invokeCall.getArgumentNodes().get(0));
                }
            }
        } else if (irBinaryImplNode.getLeftNode() instanceof BinaryImplNode) {
            // <variableName>['<field>'] and <variableName>.<field> syntax
            BinaryImplNode left = (BinaryImplNode) irBinaryImplNode.getLeftNode();
            if (left.getLeftNode() instanceof LoadVariableNode) {
                IRDecorations.IRDName name = left.getLeftNode().getDecoration(IRDecorations.IRDName.class);
                if (name != null && name.getValue().equals(variableName)) {
                    return constantNodeOrNull(left.getRightNode());
                }
            }
        }
        return null;
    }

    private ConstantNode constantNodeOrNull(IRNode node) {
        if (node instanceof CastNode) {
            // sometimes the constant nodes are wrapped in a Cast, just skip it in this case
            node = ((CastNode) node).getChildNode();
        }
        if (node instanceof ConstantNode) {
            return (ConstantNode) node;
        } else {
            return null;
        }
    }

    private boolean rightChildIsValueAccess(BinaryImplNode irBinaryImplNode) {
        if (irBinaryImplNode.getRightNode() instanceof LoadDotDefNode) {
            // .value syntax
            IRDecorations.IRDValue rightNodeValue = irBinaryImplNode.getRightNode().getDecoration(IRDecorations.IRDValue.class);
            return rightNodeValue != null && rightNodeValue.getValue().equals("value");
        } else if (irBinaryImplNode.getRightNode() instanceof InvokeCallDefNode) {
            // .getValue() syntax
            IRDecorations.IRDName rightNodeName = irBinaryImplNode.getRightNode().getDecoration(IRDecorations.IRDName.class);
            return rightNodeName != null && rightNodeName.getValue().equals("getValue");
        } else {
            return false;
        }
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

        scope.consume((rhs, lhs) -> {
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

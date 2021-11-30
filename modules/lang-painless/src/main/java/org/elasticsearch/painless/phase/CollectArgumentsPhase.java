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
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.LoadDotDefNode;
import org.elasticsearch.painless.ir.LoadVariableNode;
import org.elasticsearch.painless.ir.StoreVariableNode;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.spi.annotation.CollectArgumentAnnotation;
import org.elasticsearch.painless.symbol.CollectArgumentsScope;
import org.elasticsearch.painless.symbol.IRDecorations;
import org.elasticsearch.painless.symbol.IRDecorations.IRDName;
import org.elasticsearch.queryableexpression.QueryableExpressionBuilder;

/**
 * Collects arguments for methods annotated with {@link CollectArgumentAnnotation}.
 */
public class CollectArgumentsPhase extends IRTreeBaseVisitor<CollectArgumentsScope> {

    @Override
    public void visitFunction(FunctionNode irFunctionNode, CollectArgumentsScope collectArgumentsScope) {
        if (irFunctionNode.getDecorationValue(IRDName.class).equals("execute")) {
            super.visitFunction(irFunctionNode, collectArgumentsScope);
        }
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, CollectArgumentsScope scope) {
        PainlessClassBinding irdBinding = irInvokeCallMemberNode.getDecorationValue(IRDecorations.IRDClassBinding.class);

        if (irdBinding != null) {
            CollectArgumentAnnotation collectArgument = (CollectArgumentAnnotation) irdBinding.annotations.get(
                CollectArgumentAnnotation.class
            );
            if (collectArgument != null) {
                scope.startCollecting(collectArgument.target);
                super.visitInvokeCallMember(irInvokeCallMemberNode, scope);
                scope.stopCollecting(collectArgument.target);
                return;
            }
        }

        if (false == scope.unqueryable()) {
            super.visitInvokeCallMember(irInvokeCallMemberNode, scope);
        }
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, CollectArgumentsScope scope) {
        if (irBinaryImplNode.getLeftNode() instanceof BinaryImplNode) {
            ConstantNode docLookupKeyNode = lookupKeyForMapAccessOnVariable("doc", (BinaryImplNode) irBinaryImplNode.getLeftNode(), scope);
            if (docLookupKeyNode != null && rightChildIsValueAccess(irBinaryImplNode)) {
                Object value = docLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
                if (value instanceof String) {
                    if (scope.push(QueryableExpressionBuilder.field((String) value))) {
                        return;
                    }
                }
            }
        }

        ConstantNode paramsLookupKeyNode = lookupKeyForMapAccessOnVariable("params", irBinaryImplNode, scope);
        if (paramsLookupKeyNode != null) {
            Object value = paramsLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
            if (value instanceof String) {
                if (scope.push(QueryableExpressionBuilder.param((String) value))) {
                    return;
                }
            }
        }

        if (irBinaryImplNode.getRightNode() instanceof InvokeCallDefNode) {
            InvokeCallDefNode call = (InvokeCallDefNode) irBinaryImplNode.getRightNode();
            String name = call.getDecorationValue(IRDName.class);
            if (name.equals("substring")) {
                /*
                 * The left node push the thing we're substringing
                 * the stack. Ignore the right node because that's the
                 * arguments and we're not using those.
                 */
                irBinaryImplNode.getLeftNode().visit(this, scope);
                scope.consume(target -> QueryableExpressionBuilder.substring(target));
                return;
            }
        }

        super.visitBinaryImpl(irBinaryImplNode, scope);
        scope.consume(e -> QueryableExpressionBuilder.unknownOp(e));
    }

    /**
     * Checks whether a BinaryImplNode references a variable `variableName` on which a Map lookup is performed.
     * Returns the lookup key if it is a constant.
     */
    private ConstantNode lookupKeyForMapAccessOnVariable(
        String variableName,
        BinaryImplNode irBinaryImplNode,
        CollectArgumentsScope scope
    ) {
        if (irBinaryImplNode.getLeftNode() instanceof LoadVariableNode) {
            // <variableName>.get('<field>') syntax
            IRDecorations.IRDName name = irBinaryImplNode.getLeftNode().getDecoration(IRDecorations.IRDName.class);
            if (name != null
                && name.getValue().equals(variableName)
                && scope.isVariableAssigned(variableName) == false
                && irBinaryImplNode.getRightNode() instanceof InvokeCallNode) {
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
                if (name != null && name.getValue().equals(variableName) && scope.isVariableAssigned(variableName) == false) {
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
    public void visitConstant(ConstantNode irConstantNode, CollectArgumentsScope scope) {
        Object value = irConstantNode.getDecorationValue(IRDecorations.IRDConstant.class);
        if (scope.push(QueryableExpressionBuilder.constant(value))) {
            return;
        } else {
            super.visitConstant(irConstantNode, scope);
        }
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, CollectArgumentsScope scope) {
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
                return QueryableExpressionBuilder.unknownOp(lhs, rhs);
            }
        });
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, CollectArgumentsScope scope) {
        String name = irDeclarationNode.getDecorationString(IRDName.class);

        scope.removeVariableField(name);

        if (irDeclarationNode.getExpressionNode() instanceof BinaryImplNode) {
            BinaryImplNode irBinaryImplNode = (BinaryImplNode) irDeclarationNode.getExpressionNode();
            ExpressionNode left = irBinaryImplNode.getLeftNode();
            if (left instanceof BinaryImplNode) {
                ConstantNode docLookupKeyNode = lookupKeyForMapAccessOnVariable("doc", (BinaryImplNode) left, scope);
                if (docLookupKeyNode != null && rightChildIsValueAccess(irBinaryImplNode)) {
                    Object value = docLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
                    if (value instanceof String) {
                        scope.putVariableField(name, (String) value);
                    }
                }
            }
        }

        super.visitDeclaration(irDeclarationNode, scope);
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, CollectArgumentsScope scope) {
        String name = irStoreVariableNode.getDecorationString(IRDName.class);

        if ("doc".equals(name) || "params".equals(name)) {
            scope.setVariableAssigned(name);
        }

        scope.removeVariableField(name);

        if (irStoreVariableNode.getChildNode() instanceof BinaryImplNode) {
            BinaryImplNode irBinaryImplNode = (BinaryImplNode) irStoreVariableNode.getChildNode();
            ExpressionNode left = irBinaryImplNode.getLeftNode();
            if (left instanceof BinaryImplNode) {
                ConstantNode docLookupKeyNode = lookupKeyForMapAccessOnVariable("doc", (BinaryImplNode) left, scope);
                if (docLookupKeyNode != null && rightChildIsValueAccess(irBinaryImplNode)) {
                    Object value = docLookupKeyNode.getDecorationValue(IRDecorations.IRDConstant.class);
                    if (value instanceof String) {
                        scope.putVariableField(name, (String) value);
                    }
                }
            }
        }

        super.visitStoreVariable(irStoreVariableNode, scope);
    }

    @Override
    public void visitLoadVariable(LoadVariableNode irLoadVariableNode, CollectArgumentsScope scope) {
        String name = irLoadVariableNode.getDecorationString(IRDName.class);
        String field = scope.getVariableField(name);

        if (field != null) {
            scope.push(QueryableExpressionBuilder.field(field));
        }
    }
}

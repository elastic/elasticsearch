/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.function.Consumer;

/**
 * Phases that changes ==/!= to use String.equals when one side is a constant string
 */
public class DefaultEqualityMethodOptimizationPhase extends IRExpressionModifyingVisitor {

    private final ScriptScope scriptScope;

    public DefaultEqualityMethodOptimizationPhase(ScriptScope scriptScope) {
        this.scriptScope = scriptScope;
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, Consumer<ExpressionNode> scope) {
        super.visitComparison(irComparisonNode, scope);

        Operation op = irComparisonNode.getOperation();
        if (op == Operation.EQ || op == Operation.NE) {
            ExpressionNode constantNode = null;
            ExpressionNode argumentNode = null;
            if (irComparisonNode.getLeftNode() instanceof ConstantNode leftConstant && leftConstant.getConstant() instanceof String) {
                constantNode = irComparisonNode.getLeftNode();
                argumentNode = irComparisonNode.getRightNode();
            } else if (irComparisonNode.getRightNode() instanceof ConstantNode rightConstant
                && rightConstant.getConstant() instanceof String) {
                    // it's ok to reorder these, RHS is a constant that has no effect on execution
                    constantNode = irComparisonNode.getRightNode();
                    argumentNode = irComparisonNode.getLeftNode();
                }

            ExpressionNode node = null;
            Location loc = irComparisonNode.getLocation();
            if (constantNode != null) {
                // call String.equals directly
                InvokeCallNode invoke = new InvokeCallNode(loc);
                PainlessMethod method = scriptScope.getPainlessLookup().lookupPainlessMethod(String.class, false, "equals", 1);
                invoke.setMethod(method);
                invoke.setBox(String.class);
                invoke.addArgumentNode(argumentNode);
                invoke.setExpressionType(boolean.class);

                BinaryImplNode call = new BinaryImplNode(loc);
                call.setLeftNode(constantNode);
                call.setRightNode(invoke);
                call.setExpressionType(boolean.class);

                node = call;
            }

            if (node != null) {
                if (op == Operation.NE) {
                    UnaryMathNode not = new UnaryMathNode(loc);
                    not.setChildNode(node);
                    not.setOperation(Operation.NOT);
                    not.setExpressionType(boolean.class);
                    node = not;
                }

                // replace the comparison with this node
                scope.accept(node);
            }
        }
    }

}

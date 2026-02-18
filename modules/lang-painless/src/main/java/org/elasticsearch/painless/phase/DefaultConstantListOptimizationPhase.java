/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstant;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Optimizes constant list literals that are immediately consumed by a read-only method call
 * (e.g. {@code ['a','b','c'].contains(x)}) by hoisting the list into a static constant field.
 * <p>
 * This avoids allocating and populating a new {@link ArrayList} on every invocation. The
 * optimization is only applied when the list is provably never escaped — it is used inline
 * as the receiver of a single qualifying method call.
 * <p>
 * Bare list literals assigned to variables (e.g. {@code def x = [1, 2, 3]}) are left untouched
 * because the user may later mutate the list.
 */
public class DefaultConstantListOptimizationPhase extends IRExpressionModifyingVisitor {

    private static final Set<String> QUALIFYING_METHODS = Set.of("contains");

    @Override
    public void visitBinaryImpl(final BinaryImplNode irBinaryImplNode, final Consumer<ExpressionNode> scope) {
        super.visitBinaryImpl(irBinaryImplNode, scope);

        final ExpressionNode leftNode = irBinaryImplNode.getLeftNode();
        final ExpressionNode rightNode = irBinaryImplNode.getRightNode();

        if (leftNode instanceof ListInitializationNode listInitNode && rightNode instanceof InvokeCallNode invokeCallNode) {
            final String methodName = invokeCallNode.getMethod().javaMethod().getName();
            if (QUALIFYING_METHODS.contains(methodName) == false) {
                return;
            }

            final List<Object> constants = extractConstants(listInitNode);
            if (constants == null) {
                return;
            }

            final List<Object> constantList = new ArrayList<>(constants);

            final ConstantNode replacement = new ConstantNode(listInitNode.getLocation());
            replacement.attachDecoration(new IRDConstant(constantList));
            replacement.attachDecoration(new IRDExpressionType(ArrayList.class));

            irBinaryImplNode.setLeftNode(replacement);
        }
    }

    /**
     * Extracts constant values from all argument nodes of a {@link ListInitializationNode},
     * unwrapping {@link CastNode} wrappers as needed. Returns {@code null} if any argument
     * is not a constant.
     */
    private static List<Object> extractConstants(ListInitializationNode listInitNode) {
        List<Object> constants = new ArrayList<>(listInitNode.getArgumentNodes().size());
        for (ExpressionNode argumentNode : listInitNode.getArgumentNodes()) {
            Object constant = extractConstant(argumentNode);
            if (constant == null) {
                return null;
            }
            constants.add(constant);
        }
        return constants;
    }

    private static Object extractConstant(ExpressionNode node) {
        if (node instanceof ConstantNode) {
            return node.getDecorationValue(IRDConstant.class);
        }
        if (node instanceof CastNode castNode) {
            ExpressionNode child = castNode.getChildNode();
            if (child instanceof ConstantNode) {
                return child.getDecorationValue(IRDConstant.class);
            }
        }
        return null;
    }
}

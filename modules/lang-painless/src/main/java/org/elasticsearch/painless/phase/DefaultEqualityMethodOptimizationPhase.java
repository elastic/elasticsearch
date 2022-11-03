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
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.IRDecorations;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.Objects;

public class DefaultEqualityMethodOptimizationPhase extends UserTreeBaseVisitor<ScriptScope> {

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComp = (ComparisonNode)scriptScope.getDecoration(userCompNode, Decorations.IRNodeDecoration.class).irNode();

        if (userCompNode.getOperation() == Operation.EQ || userCompNode.getOperation() == Operation.NE) {
            // if left type is known to be a String, use Object.equals instead (this handles nulls)
            Class<?> leftType = irComp.getLeftNode().getDecorationValue(IRDecorations.IRDExpressionType.class);
            if (leftType == String.class) {
                StaticNode objects = new StaticNode(irComp.getLocation());
                objects.attachDecoration(new IRDecorations.IRDExpressionType(Objects.class));

                InvokeCallNode invoke = new InvokeCallNode(irComp.getLocation());
                PainlessMethod method = scriptScope.getPainlessLookup().lookupPainlessMethod(
                    Objects.class, true, "equals", 2);
                invoke.setMethod(method);
                invoke.addArgumentNode(irComp.getLeftNode());
                invoke.addArgumentNode(irComp.getRightNode());
                invoke.attachDecoration(new IRDecorations.IRDExpressionType(boolean.class));

                BinaryImplNode call = new BinaryImplNode(irComp.getLocation());
                call.setLeftNode(objects);
                call.setRightNode(invoke);
                call.attachDecoration(new IRDecorations.IRDExpressionType(boolean.class));

                ExpressionNode node = call;

                if (userCompNode.getOperation() == Operation.NE) {
                    UnaryMathNode not = new UnaryMathNode(irComp.getLocation());
                    not.setChildNode(node);
                    not.attachDecoration(new IRDecorations.IRDOperation(Operation.NOT));
                    not.attachDecoration(new IRDecorations.IRDExpressionType(boolean.class));
                    node = not;
                }

                scriptScope.putDecoration(userCompNode, new Decorations.IRNodeDecoration(node));
            }
        }

        userCompNode.visitChildren(this, scriptScope);
    }

}

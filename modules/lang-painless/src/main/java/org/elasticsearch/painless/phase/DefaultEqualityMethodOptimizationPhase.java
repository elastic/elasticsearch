/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Utility;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.symbol.Decorations.IRNodeDecoration;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDOperation;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.Objects;

/**
 * Phases that changes ==/!= to use Objects.equals when we know the type of one side
 */
public class DefaultEqualityMethodOptimizationPhase extends UserTreeBaseVisitor<ScriptScope> {

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComp = (ComparisonNode)scriptScope.getDecoration(userCompNode, IRNodeDecoration.class).irNode();

        if (userCompNode.getOperation() == Operation.EQ || userCompNode.getOperation() == Operation.NE) {
            Class<?> staticClass = null;
            String methodName = null;

            // if one side is know to be a String, use a method that calls Object.equals (that also handles nulls)
            Class<?> leftType = irComp.getLeftNode().getDecorationValue(IRDExpressionType.class);
            Class<?> rightType = irComp.getRightNode().getDecorationValue(IRDExpressionType.class);
            if (leftType == String.class) {
                staticClass = Objects.class;
                methodName = "equals";
            }
            else if (rightType == String.class) {
                staticClass = Utility.class;
                methodName = "equalsRHS";
            }

            if (staticClass != null) {
                StaticNode objects = new StaticNode(irComp.getLocation());
                objects.attachDecoration(new IRDExpressionType(staticClass));

                InvokeCallNode invoke = new InvokeCallNode(irComp.getLocation());
                PainlessMethod method = scriptScope.getPainlessLookup().lookupPainlessMethod(
                    staticClass, true, methodName, 2);
                invoke.setMethod(method);
                invoke.addArgumentNode(irComp.getLeftNode());
                invoke.addArgumentNode(irComp.getRightNode());
                invoke.attachDecoration(new IRDExpressionType(boolean.class));

                BinaryImplNode call = new BinaryImplNode(irComp.getLocation());
                call.setLeftNode(objects);
                call.setRightNode(invoke);
                call.attachDecoration(new IRDExpressionType(boolean.class));

                ExpressionNode node = call;

                if (userCompNode.getOperation() == Operation.NE) {
                    UnaryMathNode not = new UnaryMathNode(irComp.getLocation());
                    not.setChildNode(node);
                    not.attachDecoration(new IRDOperation(Operation.NOT));
                    not.attachDecoration(new IRDExpressionType(boolean.class));
                    node = not;
                }

                scriptScope.putDecoration(userCompNode, new IRNodeDecoration(node));
            }
        }

        userCompNode.visitChildren(this, scriptScope);
    }

}

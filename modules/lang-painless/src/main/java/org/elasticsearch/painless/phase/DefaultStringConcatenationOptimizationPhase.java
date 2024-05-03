/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;

public class DefaultStringConcatenationOptimizationPhase extends IRTreeBaseVisitor<Void> {

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Void scope) {
        int i = 0;

        while (i < irStringConcatenationNode.getArgumentNodes().size()) {
            ExpressionNode irArgumentNode = irStringConcatenationNode.getArgumentNodes().get(i);

            if (irArgumentNode instanceof StringConcatenationNode scn) {
                irStringConcatenationNode.getArgumentNodes().remove(i);
                irStringConcatenationNode.getArgumentNodes().addAll(i, scn.getArgumentNodes());
            } else {
                i++;
            }
        }

        for (ExpressionNode argumentNode : irStringConcatenationNode.getArgumentNodes()) {
            argumentNode.visit(this, scope);
        }
    }
}

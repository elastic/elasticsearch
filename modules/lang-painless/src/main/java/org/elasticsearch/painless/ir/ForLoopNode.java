/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class ForLoopNode extends ConditionNode {

    /* ---- begin tree structure ---- */

    private IRNode initializerNode;
    private ExpressionNode afterthoughtNode;

    public void setInitialzerNode(IRNode initializerNode) {
        this.initializerNode = initializerNode;
    }

    public IRNode getInitializerNode() {
        return initializerNode;
    }

    public void setAfterthoughtNode(ExpressionNode afterthoughtNode) {
        this.afterthoughtNode = afterthoughtNode;
    }

    public ExpressionNode getAfterthoughtNode() {
        return afterthoughtNode;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        if (initializerNode != null) {
            initializerNode.visit(irTreeVisitor, scope);
        }

        if (getConditionNode() != null) {
            getConditionNode().visit(irTreeVisitor, scope);
        }

        if (afterthoughtNode != null) {
            afterthoughtNode.visit(irTreeVisitor, scope);
        }

        if (getBlockNode() != null) {
            getBlockNode().visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public ForLoopNode(Location location) {
        super(location);
    }

}

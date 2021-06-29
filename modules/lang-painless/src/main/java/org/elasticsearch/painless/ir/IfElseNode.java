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

public class IfElseNode extends ConditionNode {

    /* ---- begin tree structure ---- */

    private BlockNode elseBlockNode;

    public void setElseBlockNode(BlockNode elseBlockNode) {
        this.elseBlockNode = elseBlockNode;
    }

    public BlockNode getElseBlockNode() {
        return elseBlockNode;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitIfElse(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getConditionNode().visit(irTreeVisitor, scope);
        getBlockNode().visit(irTreeVisitor, scope);
        getElseBlockNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public IfElseNode(Location location) {
        super(location);
    }

}

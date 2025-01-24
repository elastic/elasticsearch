/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class ForEachLoopNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private ConditionNode conditionNode;

    public void setConditionNode(ConditionNode conditionNode) {
        this.conditionNode = conditionNode;
    }

    public ConditionNode getConditionNode() {
        return conditionNode;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForEachLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        conditionNode.visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public ForEachLoopNode(Location location) {
        super(location);
    }

}

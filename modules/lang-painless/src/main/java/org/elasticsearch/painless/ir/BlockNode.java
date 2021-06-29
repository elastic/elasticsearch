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

import java.util.ArrayList;
import java.util.List;

public class BlockNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private final List<StatementNode> statementNodes = new ArrayList<>();

    public void addStatementNode(StatementNode statementNode) {
        statementNodes.add(statementNode);
    }

    public List<StatementNode> getStatementsNodes() {
        return statementNodes;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitBlock(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (StatementNode statementNode : statementNodes) {
            statementNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public BlockNode(Location location) {
        super(location);
    }

}

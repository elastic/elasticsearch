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

public class DeclarationBlockNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private final List<DeclarationNode> declarationNodes = new ArrayList<>();

    public void addDeclarationNode(DeclarationNode declarationNode) {
        declarationNodes.add(declarationNode);
    }

    public List<DeclarationNode> getDeclarationsNodes() {
        return declarationNodes;
    }

    /* ---- end tree structure, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitDeclarationBlock(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (DeclarationNode declarationNode : declarationNodes) {
            declarationNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public DeclarationBlockNode(Location location) {
        super(location);
    }

}

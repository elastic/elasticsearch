/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.phase.UserTreeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents the try block as part of a try-catch block.
 */
public class STry extends AStatement {

    private final SBlock blockNode;
    private final List<SCatch> catchNodes;

    public STry(int identifier, Location location, SBlock blockNode, List<SCatch> catchNodes) {
        super(identifier, location);

        this.blockNode = blockNode;
        this.catchNodes = Collections.unmodifiableList(Objects.requireNonNull(catchNodes));
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    public List<SCatch> getCatchNodes() {
        return catchNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitTry(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }

        for (SCatch catchNode : catchNodes) {
            catchNode.visit(userTreeVisitor, scope);
        }
    }
}

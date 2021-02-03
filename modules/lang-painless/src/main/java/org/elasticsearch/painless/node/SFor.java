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

/**
 * Represents a for loop.
 */
public class SFor extends AStatement {

    private final ANode initializerNode;
    private final AExpression conditionNode;
    private final AExpression afterthoughtNode;
    private final SBlock blockNode;

    public SFor(int identifier, Location location,
            ANode initializerNode, AExpression conditionNode, AExpression afterthoughtNode, SBlock blockNode) {

        super(identifier, location);

        this.initializerNode = initializerNode;
        this.conditionNode = conditionNode;
        this.afterthoughtNode = afterthoughtNode;
        this.blockNode = blockNode;
    }

    public ANode getInitializerNode() {
        return initializerNode;
    }

    public AExpression getConditionNode() {
        return conditionNode;
    }

    public AExpression getAfterthoughtNode() {
        return afterthoughtNode;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitFor(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (initializerNode != null) {
            initializerNode.visit(userTreeVisitor, scope);
        }

        if (conditionNode != null) {
            conditionNode.visit(userTreeVisitor, scope);
        }

        if (afterthoughtNode != null) {
            afterthoughtNode.visit(userTreeVisitor, scope);
        }

        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }
    }
}

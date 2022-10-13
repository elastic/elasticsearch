/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.phase.UserTreeVisitor;

import java.util.Objects;

/**
 * Represents an assignment with the lhs and rhs as child nodes.
 */
public class EAssignment extends AExpression {

    private final AExpression leftNode;
    private final AExpression rightNode;
    private final boolean postIfRead;
    private final Operation operation;

    public EAssignment(
        int identifier,
        Location location,
        AExpression leftNode,
        AExpression rightNode,
        boolean postIfRead,
        Operation operation
    ) {

        super(identifier, location);

        this.leftNode = Objects.requireNonNull(leftNode);
        this.rightNode = Objects.requireNonNull(rightNode);
        this.postIfRead = postIfRead;
        this.operation = operation;
    }

    public AExpression getLeftNode() {
        return leftNode;
    }

    public AExpression getRightNode() {
        return rightNode;
    }

    public boolean postIfRead() {
        return postIfRead;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitAssignment(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        leftNode.visit(userTreeVisitor, scope);
        rightNode.visit(userTreeVisitor, scope);
    }
}

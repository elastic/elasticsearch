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
 * Represents a unary math expression.
 */
public class EUnary extends AExpression {

    private final AExpression childNode;
    private final Operation operation;

    public EUnary(int identifier, Location location, AExpression childNode, Operation operation) {
        super(identifier, location);

        this.childNode = Objects.requireNonNull(childNode);
        this.operation = Objects.requireNonNull(operation);
    }

    public AExpression getChildNode() {
        return childNode;
    }

    public Operation getOperation() {
        return operation;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitUnary(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        childNode.visit(userTreeVisitor, scope);
    }
}

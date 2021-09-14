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

import java.util.Objects;

/**
 * Represents a throw statement.
 */
public class SThrow extends AStatement {

    private final AExpression expressionNode;

    public SThrow(int identifier, Location location, AExpression expressionNode) {
        super(identifier, location);

        this.expressionNode = Objects.requireNonNull(expressionNode);
    }

    public AExpression getExpressionNode() {
        return expressionNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitThrow(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        expressionNode.visit(userTreeVisitor, scope);
    }
}

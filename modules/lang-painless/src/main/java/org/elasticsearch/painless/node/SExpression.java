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
 * Represents the top-level node for an expression as a statement.
 */
public class SExpression extends AStatement {

    private final AExpression statementNode;

    public SExpression(int identifier, Location location, AExpression statementNode) {
        super(identifier, location);

        this.statementNode = Objects.requireNonNull(statementNode);
    }

    public AExpression getStatementNode() {
        return statementNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitExpression(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        statementNode.visit(userTreeVisitor, scope);
    }
}

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
 * Represents a set of statements as a branch of control-flow.
 */
public class SBlock extends AStatement {

    private final List<AStatement> statementNodes;

    public SBlock(int identifier, Location location, List<AStatement> statementNodes) {
        super(identifier, location);

        this.statementNodes = Collections.unmodifiableList(Objects.requireNonNull(statementNodes));
    }

    public List<AStatement> getStatementNodes() {
        return statementNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitBlock(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (AStatement statementNode: statementNodes) {
            statementNode.visit(userTreeVisitor, scope);
        }
    }
}

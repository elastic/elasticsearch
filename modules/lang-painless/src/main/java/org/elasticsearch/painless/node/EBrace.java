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
 * Represents an array load/store and defers to a child subnode.
 */
public class EBrace extends AExpression {

    private final AExpression prefixNode;
    private final AExpression indexNode;

    public EBrace(int identifier, Location location, AExpression prefixNode, AExpression indexNode) {
        super(identifier, location);

        this.prefixNode = Objects.requireNonNull(prefixNode);
        this.indexNode = Objects.requireNonNull(indexNode);
    }

    public AExpression getPrefixNode() {
        return prefixNode;
    }

    public AExpression getIndexNode() {
        return indexNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitBrace(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        prefixNode.visit(userTreeVisitor, scope);
        indexNode.visit(userTreeVisitor, scope);
    }
}

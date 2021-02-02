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
 * Represents a field load/store and defers to a child subnode.
 */
public class EDot extends AExpression {

    private final AExpression prefixNode;
    private final String index;
    private final boolean isNullSafe;

    public EDot(int identifier, Location location, AExpression prefixNode, String index, boolean isNullSafe) {
        super(identifier, location);

        this.prefixNode = Objects.requireNonNull(prefixNode);
        this.index = Objects.requireNonNull(index);
        this.isNullSafe = isNullSafe;
    }

    public AExpression getPrefixNode() {
        return prefixNode;
    }

    public String getIndex() {
        return index;
    }

    public boolean isNullSafe() {
        return isNullSafe;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitDot(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        prefixNode.visit(userTreeVisitor, scope);
    }
}

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
 * Represents an explicit cast.
 */
public class EExplicit extends AExpression {

    private final String canonicalTypeName;
    private final AExpression childNode;

    public EExplicit(int identifier, Location location, String canonicalTypeName, AExpression childNode) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.childNode = Objects.requireNonNull(childNode);
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public AExpression getChildNode() {
        return childNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitExplicit(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        childNode.visit(userTreeVisitor, scope);
    }
}

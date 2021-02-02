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
 * Represents an array instantiation.
 */
public class ENewArray extends AExpression {

    private final String canonicalTypeName;
    private final List<AExpression> valueNodes;
    private final boolean isInitializer;

    public ENewArray(int identifier, Location location, String canonicalTypeName, List<AExpression> valueNodes, boolean isInitializer) {
        super(identifier, location);

        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
        this.isInitializer = isInitializer;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    public boolean isInitializer() {
        return isInitializer;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitNewArray(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (AExpression valueNode : valueNodes) {
            valueNode.visit(userTreeVisitor, scope);
        }
    }
}

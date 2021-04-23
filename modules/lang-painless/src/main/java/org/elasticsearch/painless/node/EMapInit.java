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
 * Represents a map initialization shortcut.
 */
public class EMapInit extends AExpression {

    private final List<AExpression> keyNodes;
    private final List<AExpression> valueNodes;

    public EMapInit(int identifier, Location location, List<AExpression> keyNodes, List<AExpression> valueNodes) {
        super(identifier, location);

        this.keyNodes = Collections.unmodifiableList(Objects.requireNonNull(keyNodes));
        this.valueNodes = Collections.unmodifiableList(Objects.requireNonNull(valueNodes));
    }

    public List<AExpression> getKeyNodes() {
        return keyNodes;
    }

    public List<AExpression> getValueNodes() {
        return valueNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitMapInit(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (AExpression keyNode : keyNodes) {
            keyNode.visit(userTreeVisitor, scope);
        }

        for (AExpression valueNode : valueNodes) {
            valueNode.visit(userTreeVisitor, scope);
        }
    }
}

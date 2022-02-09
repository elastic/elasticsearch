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
 * The root of all Painless trees.  Contains a series of statements.
 */
public class SClass extends ANode {

    private final List<SFunction> functionNodes;

    public SClass(int identifier, Location location, List<SFunction> functionNodes) {
        super(identifier, location);

        this.functionNodes = Collections.unmodifiableList(Objects.requireNonNull(functionNodes));
    }

    public List<SFunction> getFunctionNodes() {
        return functionNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitClass(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (SFunction functionNode : functionNodes) {
            functionNode.visit(userTreeVisitor, scope);
        }
    }
}

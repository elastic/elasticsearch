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

/**
 * Represents a series of declarations.
 */
public class SDeclBlock extends AStatement {

    private final List<SDeclaration> declarationNodes;

    public SDeclBlock(int identifier, Location location, List<SDeclaration> declarationNodes) {
        super(identifier, location);

        this.declarationNodes = Collections.unmodifiableList(declarationNodes);
    }

    public List<SDeclaration> getDeclarationNodes() {
        return declarationNodes;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitDeclBlock(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        for (SDeclaration declarationNode : declarationNodes) {
            declarationNode.visit(userTreeVisitor, scope);
        }
    }
}

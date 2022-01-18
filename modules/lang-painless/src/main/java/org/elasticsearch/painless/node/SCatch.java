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
 * Represents a catch block as part of a try-catch block.
 */
public class SCatch extends AStatement {

    private final Class<?> baseException;
    private final String canonicalTypeName;
    private final String symbol;
    private final SBlock blockNode;

    public SCatch(int identifier, Location location, Class<?> baseException, String canonicalTypeName, String symbol, SBlock blockNode) {
        super(identifier, location);

        this.baseException = Objects.requireNonNull(baseException);
        this.canonicalTypeName = Objects.requireNonNull(canonicalTypeName);
        this.symbol = Objects.requireNonNull(symbol);
        this.blockNode = blockNode;
    }

    public Class<?> getBaseException() {
        return baseException;
    }

    public String getCanonicalTypeName() {
        return canonicalTypeName;
    }

    public String getSymbol() {
        return symbol;
    }

    public SBlock getBlockNode() {
        return blockNode;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitCatch(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        if (blockNode != null) {
            blockNode.visit(userTreeVisitor, scope);
        }
    }
}

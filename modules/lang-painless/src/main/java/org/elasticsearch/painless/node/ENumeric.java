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
 * Represents a non-decimal numeric constant.
 */
public class ENumeric extends AExpression {

    private final String numeric;
    private final int radix;

    public ENumeric(int identifier, Location location, String numeric, int radix) {
        super(identifier, location);

        this.numeric = Objects.requireNonNull(numeric);
        this.radix = radix;
    }

    public String getNumeric() {
        return numeric;
    }

    public int getRadix() {
        return radix;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitNumeric(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        // terminal node; no children
    }
}

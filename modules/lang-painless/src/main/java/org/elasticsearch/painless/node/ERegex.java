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
 * Represents a regex constant. All regexes are constants.
 */
public class ERegex extends AExpression {

    private final String pattern;
    private final String flags;

    public ERegex(int identifier, Location location, String pattern, String flags) {
        super(identifier, location);

        this.pattern = Objects.requireNonNull(pattern);
        this.flags = Objects.requireNonNull(flags);
    }

    public String getPattern() {
        return pattern;
    }

    public String getFlags() {
        return flags;
    }

    @Override
    public <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        userTreeVisitor.visitRegex(this, scope);
    }

    @Override
    public <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope) {
        // terminal node; no children
    }
}

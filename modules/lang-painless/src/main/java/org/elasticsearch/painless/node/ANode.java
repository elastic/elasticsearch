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
 * The superclass for all nodes.
 */
public abstract class ANode {

    private final int identifier;
    private final Location location;

    /**
     * Standard constructor with location used for error tracking.
     */
    public ANode(int identifier, Location location) {
        this.identifier = identifier;
        this.location = Objects.requireNonNull(location);
    }

    /**
     * An unique id for the node. This is guaranteed to be smallest possible
     * value so it can be used for value caching in an array instead of a
     * hash look up.
     */
    public int getIdentifier() {
        return identifier;
    }

    /**
     * The identifier of the script and character offset used for debugging and errors.
     */
    public Location getLocation() {
        return location;
    }

    /**
     * Create an error with location information pointing to this node.
     */
    public RuntimeException createError(RuntimeException exception) {
        return location.createError(exception);
    }

    /**
     * Callback to visit a user tree node.
     */
    public abstract <Scope> void visit(UserTreeVisitor<Scope> userTreeVisitor, Scope scope);

    /**
     * Visits all child user tree nodes for this user tree node.
     */
    public abstract <Scope> void visitChildren(UserTreeVisitor<Scope> userTreeVisitor, Scope scope);
}

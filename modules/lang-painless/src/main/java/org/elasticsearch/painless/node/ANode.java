/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

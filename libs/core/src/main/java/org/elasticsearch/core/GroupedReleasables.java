/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.util.ArrayList;

/**
 * A mutable group of {@link Releasable}s that are released together. Declare this as the
 * try-with-resources target and {@link #add} members as they are created. Not thread-safe.
 */
public final class GroupedReleasables implements Releasable {

    private final ArrayList<Releasable> releasables;

    public GroupedReleasables() {
        this(4);
    }

    public GroupedReleasables(int sizeHint) {
        this.releasables = new ArrayList<>(sizeHint);
    }

    /**
     * Adds {@code releasable} to the group and returns it, so that a resource can be created and
     * registered in a single expression.
     */
    public <T extends Releasable> T add(T releasable) {
        releasables.add(releasable);
        return releasable;
    }

    /** The number of resources currently held. */
    public int size() {
        return releasables.size();
    }

    @Override
    public void close() {
        try {
            Releasables.close(releasables);
        } finally {
            releasables.clear();
        }
    }
}

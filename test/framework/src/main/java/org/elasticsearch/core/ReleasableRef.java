/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.transport.LeakTracker;

import java.util.Objects;

/**
 * Adapter to use a {@link RefCounted} in a try-with-resources block.
 */
public final class ReleasableRef<T extends RefCounted> implements Releasable {

    private final Releasable closeResource;
    private final T resource;

    private ReleasableRef(T resource) {
        this.resource = Objects.requireNonNull(resource);
        this.closeResource = LeakTracker.wrap(Releasables.assertOnce(resource::decRef));
    }

    @Override
    public void close() {
        closeResource.close();
    }

    public static <T extends RefCounted> ReleasableRef<T> of(T resource) {
        return new ReleasableRef<>(resource);
    }

    public T get() {
        assert resource.hasReferences() : resource + " is closed";
        return resource;
    }

    @Override
    public String toString() {
        return "ReleasableRef[" + resource + ']';
    }

}

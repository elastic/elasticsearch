/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * Adapter to use a {@link RefCounted} in a try-with-resources block.
 */
public record ReleasableRef<T extends RefCounted>(T get) implements Releasable {
    @Override
    public void close() {
        get().decRef();
    }

    public static <T extends RefCounted> ReleasableRef<T> of(T value) {
        return new ReleasableRef<>(value);
    }
}

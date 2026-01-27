/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.util.resource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * A mutable version of {@link Resource}. Anywhere a {@link Resource} is accepted in the test clusters API a {@link MutableResource} can
 * be supplied instead. Unless otherwise specified, when the {@link #update(Resource)} method is called, the backing configuration will
 * be updated in-place.
 */
public class MutableResource implements Resource {
    private final List<Consumer<? super Resource>> listeners = new ArrayList<>();
    private Resource delegate;

    private MutableResource(Resource delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputStream asStream() {
        return delegate.asStream();
    }

    public static MutableResource from(Resource delegate) {
        return new MutableResource(delegate);
    }

    public void update(Resource delegate) {
        this.delegate = delegate;
        this.listeners.forEach(listener -> listener.accept(this));
    }

    /**
     * Registers a listener that will be notified when any updates are made to this resource. This listener will receive a reference to
     * the resource with the updated value.
     *
     * @param listener action to be called on update
     */
    public synchronized void addUpdateListener(Consumer<? super Resource> listener) {
        listeners.add(listener);
    }
}

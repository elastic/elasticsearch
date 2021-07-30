/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.watcher;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Abstract resource watcher framework, which handles adding and removing listeners
 * and calling resource observer.
 */
public abstract class AbstractResourceWatcher<Listener> implements ResourceWatcher {
    private final List<Listener> listeners = new CopyOnWriteArrayList<>();
    private boolean initialized = false;

    @Override
    public void init() throws IOException {
        if (initialized == false) {
            doInit();
            initialized = true;
        }
    }

    @Override
    public void checkAndNotify() throws IOException {
        init();
        doCheckAndNotify();
    }

    /**
     * Registers new listener
     */
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    /**
     * Unregisters a listener
     */
    public void remove(Listener listener) {
        listeners.remove(listener);
    }

    /**
     * Returns a list of listeners
     */
    protected List<Listener> listeners() {
        return listeners;
    }

    /**
     * Will be called once on initialization
     */
    protected abstract void doInit() throws IOException;

    /**
     * Will be called periodically
     * <p>
     * Implementing watcher should check resource and notify all {@link #listeners()}.
     */
    protected abstract void doCheckAndNotify() throws IOException;

}

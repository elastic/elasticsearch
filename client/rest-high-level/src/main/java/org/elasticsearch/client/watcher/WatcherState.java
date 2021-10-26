/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.watcher;

public enum WatcherState {

    /**
     * The watcher plugin is not running and not functional.
     */
    STOPPED(0),

    /**
     * The watcher plugin is performing the necessary operations to get into a started state.
     */
    STARTING(1),

    /**
     * The watcher plugin is running and completely functional.
     */
    STARTED(2),

    /**
     * The watcher plugin is shutting down and not functional.
     */
    STOPPING(3);

    private final byte id;

    WatcherState(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

}


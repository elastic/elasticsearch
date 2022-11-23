/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher;

/**
 * Encapsulates the state of the watcher plugin.
 */
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

    public static WatcherState fromId(byte id) {
        switch (id) {
            case 0:
                return STOPPED;
            case 1:
                return STARTING;
            case 2:
                return STARTED;
            default: // 3
                assert id == 3 : "unknown watcher state id [" + id + "]";
                return STOPPING;
        }
    }
}

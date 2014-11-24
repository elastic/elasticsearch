/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.ElasticsearchIllegalArgumentException;

/**
 * Encapsulates the state of the alerts plugin.
 */
public enum State {

    /**
     * The alerts plugin is not running and not functional.
     */
    STOPPED(0),

    /**
     * The alerts plugin is performing the necessary operations to get into a started state.
     */
    STARTING(1),

    /**
     * The alerts plugin is running and completely functional.
     */
    STARTED(2),

    /**
     * The alerts plugin is shutting down and not functional.
     */
    STOPPING(3);

    private final byte id;

    State(int id) {
        this.id = (byte) id;
    }

    public byte getId() {
        return id;
    }

    public static State fromId(byte id) {
        switch (id) {
            case 0:
                return STOPPED;
            case 1:
                return STARTING;
            case 2:
                return STARTED;
            case 3:
                return STOPPING;
            default:
                throw new ElasticsearchIllegalArgumentException("Unknown id: " + id);
        }
    }
}

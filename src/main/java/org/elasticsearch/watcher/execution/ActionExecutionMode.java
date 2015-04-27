/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.execution;

import org.elasticsearch.watcher.WatcherException;

import java.util.Locale;

/**
 *
 */
public enum ActionExecutionMode {

    /**
     * The action will be simulated (not actually executed) and it will be throttled if needed.
     */
    SIMULATE((byte) 1),

    /**
     * The action will be simulated (not actually executed) and it will <b>not</b> be throttled.
     */
    FORCE_SIMULATE((byte) 2),

    /**
     * The action will be executed and it will be throttled if needed.
     */
    EXECUTE((byte) 3),

    /**
     * The action will be executed and it will <b>not</b> be throttled.
     */
    FORCE_EXECUTE((byte) 4),

    /**
     * The action will be skipped (it won't be executed nor simulated) - effectively it will be forcefully throttled
     */
    SKIP((byte) 5);

    private final byte id;

    ActionExecutionMode(byte id) {
        this.id = id;
    }

    public final byte id() {
        return id;
    }

    public static ActionExecutionMode resolve(byte id) {
        switch (id) {
            case 1: return SIMULATE;
            case 2: return FORCE_SIMULATE;
            case 3: return EXECUTE;
            case 4: return FORCE_EXECUTE;
            case 5: return SKIP;
        }
        throw new WatcherException("unknown action execution mode id [{}]", id);
    }

    public static ActionExecutionMode resolve(String key) {
        if (key == null) {
            return null;
        }
        switch (key.toLowerCase(Locale.ROOT)) {
            case "simulate":        return SIMULATE;
            case "force_simulate":  return FORCE_SIMULATE;
            case "execute":         return EXECUTE;
            case "force_execute":   return FORCE_EXECUTE;
            case "skip":            return SKIP;
        }
        throw new WatcherException("unknown action execution mode [{}]", key);
    }
}

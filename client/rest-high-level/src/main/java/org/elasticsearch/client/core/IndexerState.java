/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.core;

import java.util.Locale;

/**
 * IndexerState represents the internal state of the indexer.  It
 * is also persistent when changing from started/stopped in case the allocated
 * task is restarted elsewhere.
 */
public enum IndexerState {
    /** Indexer is running, but not actively indexing data (e.g. it's idle). */
    STARTED,

    /** Indexer is actively indexing data. */
    INDEXING,

    /**
     * Transition state to where an indexer has acknowledged the stop
     * but is still in process of halting.
     */
    STOPPING,

    /** Indexer is "paused" and ignoring scheduled triggers. */
    STOPPED,

    /**
     * Something (internal or external) has requested the indexer abort
     * and shutdown.
     */
    ABORTING;

    public static IndexerState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}

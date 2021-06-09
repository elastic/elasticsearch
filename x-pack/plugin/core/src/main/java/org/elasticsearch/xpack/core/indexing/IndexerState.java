/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Locale;

/**
 * IndexerState represents the internal state of the indexer.  It
 * is also persistent when changing from started/stopped in case the allocated
 * task is restarted elsewhere.
 */
public enum IndexerState implements Writeable {
    // Indexer is running, but not actively indexing data (e.g. it's idle)
    STARTED,

    // Indexer is actively indexing data
    INDEXING,

    // Transition state to where an indexer has acknowledged the stop
    // but is still in process of halting
    STOPPING,

    // Indexer is "paused" and ignoring scheduled triggers
    STOPPED,

    // Something (internal or external) has requested the indexer abort and shutdown
    ABORTING;

    public final ParseField STATE = new ParseField("job_state");

    public static IndexerState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static IndexerState fromStream(StreamInput in) throws IOException {
        return in.readEnum(IndexerState.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        IndexerState state = this;
        out.writeEnum(state);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }
}

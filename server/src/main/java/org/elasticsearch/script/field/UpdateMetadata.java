/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.script.UpdateScript;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;


/**
 * Metadata for Updates via {@link UpdateScript} with script and upsert.
 *
 * _index, _id, _routing, _version and _now (timestamp) are read-only.
 *
 * _op is read/write with valid values: NOOP ("none"), INDEX, DELETE, CREATE
 *
 * _version_type is unavailable.
 */

/**
 * Metadata for insertions done via scripted upsert with an {@link UpdateScript}
 *
 * The only metadata available is the timestamp and the Op.
 * The Op must be either "create" or "none" (Op.NOOP).
 *
 * _index, _id, _routing, _version, _version_type are unavailable.
 */
public abstract class UpdateMetadata extends Metadata {
    protected final ZonedDateTime timestamp;
    public final String TIMESTAMP = "_now";
    /**
     * The old way of spelling Op.NOOP, Metadata will read this String but does not write it.
     */
    private static final String LEGACY_NOOP_STRING = "none";

    public UpdateMetadata(Map<String, Object> ctx, Op op, long timestamp) {
        super(ctx);
        ctx.put(TIMESTAMP, timestamp);
        this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneOffset.UTC);
    }
}

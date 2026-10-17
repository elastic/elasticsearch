/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

/**
 * A query that was picked for the sample.
 *
 * @param tracked live counters that keep changing as the query is seen again, so they are read when needed and not copied
 * @param eventId {@code null} for a query picked by the sampler
 */
public record SampledQuery(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked, @Nullable String eventId) {

    public SampledQuery(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked) {
        this(fingerprint, search, tracked, null);
    }

    /**
     * An arrival kept as an event, see {@link org.elasticsearch.xpack.querysampling.sampling.EventSlice}; its weights
     * are final.
     */
    public static SampledQuery event(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked, String eventId) {
        return new SampledQuery(fingerprint, search, tracked, eventId);
    }

    public boolean isEvent() {
        return eventId != null;
    }
}

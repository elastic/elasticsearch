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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A query that was picked for the sample. Later results, such as the ground truth, are attached under an
 * {@link AttachmentKey} and are {@code null} until then.
 */
public final class SampledQuery {

    private final QueryFingerprint fingerprint;
    private final CapturedSearch search;
    private final TrackedQuery tracked;
    private final String eventId;
    private final Map<AttachmentKey<?>, Object> attachments = new ConcurrentHashMap<>();

    /**
     * @param tracked live counters that keep changing as the query is seen again, so they are read when needed and
     *                not copied
     */
    public SampledQuery(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked) {
        this(fingerprint, search, tracked, null);
    }

    private SampledQuery(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked, @Nullable String eventId) {
        this.fingerprint = fingerprint;
        this.search = search;
        this.tracked = tracked;
        this.eventId = eventId;
    }

    /**
     * An arrival kept as an event, see {@link org.elasticsearch.xpack.querysampling.sampling.EventSlice}; its weights
     * are final.
     */
    public static SampledQuery event(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked, String eventId) {
        return new SampledQuery(fingerprint, search, tracked, eventId);
    }

    @Nullable
    public String eventId() {
        return eventId;
    }

    public boolean isEvent() {
        return eventId != null;
    }

    public QueryFingerprint fingerprint() {
        return fingerprint;
    }

    public CapturedSearch search() {
        return search;
    }

    public TrackedQuery tracked() {
        return tracked;
    }

    @Nullable
    public <T> T attachment(AttachmentKey<T> key) {
        return key.type().cast(attachments.get(key));
    }

    /** Payloads are produced after the query is picked, possibly on another thread. */
    public <T> void attach(AttachmentKey<T> key, T value) {
        attachments.put(key, value);
    }
}

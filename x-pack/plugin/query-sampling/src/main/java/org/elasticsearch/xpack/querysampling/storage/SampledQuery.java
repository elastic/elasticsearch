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
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A query that was picked for the sample.
 * <p>
 * It is created when the query is picked. What a use of the sample needs to know about it beyond that,
 * such as the ground truth for recall estimation, is attached later under an {@link AttachmentKey}; until
 * then the attachment is {@code null}.
 */
public final class SampledQuery {

    private final QueryFingerprint fingerprint;
    private final CapturedSearch search;
    private final TrackedQuery tracked;
    private final Map<AttachmentKey<?>, Object> attachments = new ConcurrentHashMap<>();

    /**
     * @param fingerprint identity of the query
     * @param search      the arrival that got the query picked, with what the cluster answered at that time
     * @param tracked     live counters of the query: its multiplicity and inclusion probability keep changing
     *                    as the query is seen again, so they are read when needed and not copied
     */
    public SampledQuery(QueryFingerprint fingerprint, CapturedSearch search, TrackedQuery tracked) {
        this.fingerprint = fingerprint;
        this.search = search;
        this.tracked = tracked;
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

    /**
     * The payload attached under the key, or {@code null} if there is none yet.
     */
    @Nullable
    public <T> T attachment(AttachmentKey<T> key) {
        return key.type().cast(attachments.get(key));
    }

    /**
     * Attaches a payload, replacing the one under the same key if there is one. Payloads are usually
     * produced later than the query is picked, possibly on another thread.
     */
    public <T> void attach(AttachmentKey<T> key, T value) {
        attachments.put(key, value);
    }

    @Nullable
    public GroundTruth groundTruth() {
        return attachment(GroundTruth.KEY);
    }

    public void groundTruth(GroundTruth groundTruth) {
        attach(GroundTruth.KEY, groundTruth);
    }
}

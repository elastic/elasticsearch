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

/**
 * A query that was picked for the sample.
 * <p>
 * It is created when the query is picked and gets its ground truth later, once the exact search has been
 * run; until then {@link #groundTruth()} is {@code null}.
 */
public final class SampledQuery {

    private final QueryFingerprint fingerprint;
    private final CapturedSearch search;
    private final TrackedQuery tracked;
    private volatile GroundTruth groundTruth;

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

    @Nullable
    public GroundTruth groundTruth() {
        return groundTruth;
    }

    public void groundTruth(GroundTruth groundTruth) {
        this.groundTruth = groundTruth;
    }
}

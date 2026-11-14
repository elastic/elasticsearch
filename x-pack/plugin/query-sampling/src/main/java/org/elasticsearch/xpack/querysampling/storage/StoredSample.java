/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

/**
 * A sampled query as read back from {@link QuerySamplingIndex}; unlike a {@link SampledQuery} it is a snapshot of
 * the counters last written. {@code pickedAt} and {@code updatedAt} are epoch millis; {@code eventId} is
 * {@code null} for a query picked by the sampler.
 */
public record StoredSample(
    String samplerId,
    String fingerprint,
    CapturedSearch search,
    TrackedQuery.Weights weights,
    long pickedAt,
    long updatedAt,
    @Nullable GroundTruth groundTruth,
    @Nullable Stratum stratum,
    @Nullable Hardness hardness,
    @Nullable String eventId,
    @Nullable Selectivity selectivity
) {

    public String id() {
        return SampleRecord.documentId(samplerId, fingerprint, eventId);
    }

    public boolean isEvent() {
        return eventId != null;
    }
}

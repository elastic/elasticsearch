/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

/**
 * A sampled query as it was read back from {@link QuerySamplingIndex}. Unlike a {@link SampledQuery} it is a
 * snapshot: the counters are the ones last written, and nothing keeps changing under it.
 *
 * @param samplerId    the run of the sampler that picked the query
 * @param fingerprint  identity of the query, see {@link org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint#hex()}
 * @param search       the query and what the live search answered
 * @param weights      what is needed to weight the query when estimating over the traffic
 * @param pickedAt     milliseconds since the epoch
 * @param updatedAt    milliseconds since the epoch, when the weights were last written
 * @param groundTruth  the exact answer, or {@code null} while it has not been computed
 */
public record StoredSample(
    String samplerId,
    String fingerprint,
    CapturedSearch search,
    TrackedQuery.Weights weights,
    long pickedAt,
    long updatedAt,
    @Nullable GroundTruth groundTruth
) {

    /**
     * The id of the document, which is how it is updated.
     */
    public String id() {
        return SampleRecord.documentId(samplerId, fingerprint);
    }
}

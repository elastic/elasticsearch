/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * How a {@link SampledQuery} is laid out as a document of {@link QuerySamplingIndex}. A document is first written
 * in full when the query is picked. The weights keep changing as the query keeps arriving, so they are written
 * again, on their own, from time to time.
 */
public final class SampleRecord {

    private SampleRecord() {}

    /**
     * Identifies the query within the samples of one sampler. The sampler is part of it because two samplers
     * that picked the same query counted its arrivals separately, and their weights must not overwrite each other.
     */
    public static String documentId(String samplerId, QueryFingerprint fingerprint) {
        return samplerId + "_" + fingerprint.hex();
    }

    /**
     * The whole document, for a query that was just picked.
     */
    public static XContentBuilder document(XContentBuilder builder, String samplerId, SampledQuery sampled, long nowMillis)
        throws IOException {
        CapturedSearch search = sampled.search();
        CapturedQuery query = search.query();
        GroundTruth groundTruth = sampled.groundTruth();

        builder.startObject();
        builder.field("sampler_id", samplerId);
        builder.field("fingerprint", sampled.fingerprint().hex());
        builder.array("indices", query.indices());
        builder.field("field", query.field());
        builder.field("k", query.k());
        weights(builder, sampled.tracked().weights());
        builder.field("picked_at", nowMillis);
        builder.field("updated_at", nowMillis);
        builder.field("has_ground_truth", groundTruth != null);

        builder.startObject("query");
        builder.field("query_vector", query.queryVector());
        builder.field("num_candidates", query.numCandidates());
        if (query.visitPercentage() != null) {
            builder.field("visit_percentage", query.visitPercentage());
        }
        if (query.oversample() != null) {
            builder.field("oversample", query.oversample());
        }
        builder.startArray("filters");
        for (QueryBuilder filter : query.filters()) {
            filter.toXContent(builder, EMPTY_PARAMS);
        }
        builder.endArray();
        if (query.opaqueId() != null) {
            builder.field("opaque_id", query.opaqueId());
        }
        builder.endObject();

        builder.startObject("live_hits");
        builder.field("took_millis", search.tookMillis());
        hits(builder, "hits", search.hits());
        builder.endObject();

        if (groundTruth != null) {
            builder.startObject("ground_truth");
            hits(builder, "neighbors", groundTruth.neighbors());
            builder.endObject();
        }
        return builder.endObject();
    }

    /**
     * The fields of a document that change while the query keeps arriving, as a partial update.
     */
    public static XContentBuilder weightsUpdate(XContentBuilder builder, TrackedQuery.Weights weights, long nowMillis) throws IOException {
        builder.startObject();
        weights(builder, weights);
        builder.field("updated_at", nowMillis);
        return builder.endObject();
    }

    private static void weights(XContentBuilder builder, TrackedQuery.Weights weights) throws IOException {
        builder.field("multiplicity", weights.multiplicity());
        builder.field("weighted_multiplicity", weights.weightedMultiplicity());
        builder.field("inclusion_probability", weights.inclusionProbability());
        builder.field("seen_probability", weights.seenProbability());
        builder.field("capture_rate", weights.captureRate());
    }

    private static void hits(XContentBuilder builder, String name, Iterable<CapturedSearch.Hit> hits) throws IOException {
        builder.startArray(name);
        for (CapturedSearch.Hit hit : hits) {
            builder.startObject().field("index", hit.index()).field("id", hit.id()).field("score", hit.score()).endObject();
        }
        builder.endArray();
    }
}

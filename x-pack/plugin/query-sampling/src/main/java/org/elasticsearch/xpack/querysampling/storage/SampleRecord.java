/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.Hardness;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.Selectivity;
import org.elasticsearch.xpack.querysampling.dedup.Stratum;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.DataState;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;

/**
 * Layout of a {@link SampledQuery} as a {@link QuerySamplingIndex} document: written in full when picked, with the
 * weights rewritten on their own as the query keeps arriving.
 */
public final class SampleRecord {

    private SampleRecord() {}

    /** The sampler is part of the id so that two samplers that picked the same query do not overwrite each other's weights. */
    public static String documentId(String samplerId, QueryFingerprint fingerprint) {
        return documentId(samplerId, fingerprint.hex(), null);
    }

    public static String documentId(String samplerId, String fingerprintHex, @Nullable String eventId) {
        String id = samplerId + "_" + fingerprintHex;
        return eventId == null ? id : id + "_" + eventId;
    }

    public static XContentBuilder document(XContentBuilder builder, String samplerId, SampledQuery sampled, long nowMillis)
        throws IOException {
        CapturedSearch search = sampled.search();
        CapturedQuery query = search.query();
        TrackedQuery tracked = sampled.tracked();

        builder.startObject();
        identity(
            builder,
            samplerId,
            sampled.fingerprint().hex(),
            sampled.eventId(),
            query,
            tracked.weights(),
            tracked.stratum(),
            tracked.hardness(),
            tracked.selectivity()
        );
        builder.field("picked_at", nowMillis);
        builder.field("updated_at", nowMillis);
        builder.field("has_ground_truth", false); // the ground truth is computed later and written as an update

        query(builder, query);
        liveHits(builder, search);
        return builder.endObject();
    }

    /** The fields that a sampled query and its golden copy have in common, up to its hardness and selectivity. */
    static void identity(
        XContentBuilder builder,
        String samplerId,
        String fingerprint,
        @Nullable String eventId,
        CapturedQuery query,
        TrackedQuery.Weights weights,
        @Nullable Stratum stratum,
        @Nullable Hardness hardness,
        @Nullable Selectivity selectivity
    ) throws IOException {
        builder.field("sampler_id", samplerId);
        builder.field("fingerprint", fingerprint);
        if (eventId != null) {
            builder.field("event_id", eventId);
        }
        builder.array("indices", query.indices());
        builder.field("field", query.field());
        builder.field("k", query.k());
        weights(builder, weights);
        if (stratum != null) {
            builder.field("spatial_space", stratum.space());
            builder.field("spatial_cluster", stratum.cluster());
        }
        if (hardness != null) {
            builder.field("hardness", hardness.name().toLowerCase(Locale.ROOT));
        }
        if (selectivity != null) {
            builder.field("selectivity", selectivity.name().toLowerCase(Locale.ROOT));
        }
    }

    static void query(XContentBuilder builder, CapturedQuery query) throws IOException {
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
    }

    static void liveHits(XContentBuilder builder, CapturedSearch search) throws IOException {
        builder.startObject("live_hits");
        builder.field("took_millis", search.tookMillis());
        hits(builder, "hits", search.hits());
        builder.endObject();
    }

    public static XContentBuilder weightsUpdate(XContentBuilder builder, TrackedQuery.Weights weights, long nowMillis) throws IOException {
        builder.startObject();
        weights(builder, weights);
        builder.field("updated_at", nowMillis);
        return builder.endObject();
    }

    public static XContentBuilder groundTruthUpdate(XContentBuilder builder, GroundTruth groundTruth, long nowMillis) throws IOException {
        builder.startObject();
        groundTruth(builder, groundTruth);
        builder.field("has_ground_truth", true);
        builder.field("updated_at", nowMillis);
        return builder.endObject();
    }

    /** Only marks the document as looked at, so that documents that cannot be processed make way for others. */
    public static XContentBuilder touch(XContentBuilder builder, long nowMillis) throws IOException {
        return builder.startObject().field("updated_at", nowMillis).endObject();
    }

    /** The inverse of {@link #document}; the registry is needed to read the filters. */
    public static StoredSample parse(Map<String, Object> source, NamedXContentRegistry registry) throws IOException {
        Map<String, Object> query = map(source.get("query"));
        List<QueryBuilder> filters = new ArrayList<>();
        for (Object filter : (List<?>) query.get("filters")) {
            filters.add(parseFilter(map(filter), registry));
        }
        List<?> vector = (List<?>) query.get("query_vector");
        float[] queryVector = new float[vector.size()];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = ((Number) vector.get(i)).floatValue();
        }
        CapturedQuery captured = new CapturedQuery(
            ((List<?>) source.get("indices")).stream().map(String.class::cast).toArray(String[]::new),
            (String) source.get("field"),
            queryVector,
            ((Number) source.get("k")).intValue(),
            ((Number) query.get("num_candidates")).intValue(),
            optionalFloat(query.get("visit_percentage")),
            optionalFloat(query.get("oversample")),
            filters,
            (String) query.get("opaque_id")
        );

        Map<String, Object> liveHits = map(source.get("live_hits"));
        double captureRate = ((Number) source.get("capture_rate")).doubleValue();
        CapturedSearch search = new CapturedSearch(
            captured,
            hits(liveHits.get("hits")),
            ((Number) liveHits.get("took_millis")).longValue(),
            captureRate
        );

        TrackedQuery.Weights weights = new TrackedQuery.Weights(
            ((Number) source.get("multiplicity")).longValue(),
            ((Number) source.get("weighted_multiplicity")).doubleValue(),
            ((Number) source.get("inclusion_probability")).doubleValue(),
            captureRate
        );
        GroundTruth groundTruth = source.get("ground_truth") == null ? null : parseGroundTruth(map(source.get("ground_truth")));
        Stratum stratum = source.get("spatial_space") == null
            ? null
            : new Stratum((String) source.get("spatial_space"), ((Number) source.get("spatial_cluster")).intValue());
        Hardness hardness = source.get("hardness") == null
            ? null
            : Hardness.valueOf(((String) source.get("hardness")).toUpperCase(Locale.ROOT));
        return new StoredSample(
            (String) source.get("sampler_id"),
            (String) source.get("fingerprint"),
            search,
            weights,
            ((Number) source.get("picked_at")).longValue(),
            ((Number) source.get("updated_at")).longValue(),
            groundTruth,
            stratum,
            hardness,
            (String) source.get("event_id"),
            source.get("selectivity") == null ? null : Selectivity.valueOf(((String) source.get("selectivity")).toUpperCase(Locale.ROOT))
        );
    }

    @SuppressWarnings("unchecked") // objects of a source are maps of strings, this class is what writes them
    private static Map<String, Object> map(Object object) {
        return (Map<String, Object>) object;
    }

    private static GroundTruth parseGroundTruth(Map<String, Object> groundTruth) {
        Map<String, Object> state = groundTruth.get("data_state") == null ? null : map(groundTruth.get("data_state"));
        return new GroundTruth(
            hits(groundTruth.get("neighbors")),
            state == null || state.get("digest") == null
                ? null
                : new DataState(((Number) state.get("documents")).longValue(), ((Number) state.get("digest")).longValue())
        );
    }

    private static Float optionalFloat(Object value) {
        return value == null ? null : ((Number) value).floatValue();
    }

    private static List<CapturedSearch.Hit> hits(Object hits) {
        List<CapturedSearch.Hit> result = new ArrayList<>();
        for (Object hit : (List<?>) hits) {
            Map<String, Object> fields = map(hit);
            result.add(
                new CapturedSearch.Hit((String) fields.get("index"), (String) fields.get("id"), ((Number) fields.get("score")).floatValue())
            );
        }
        return result;
    }

    private static QueryBuilder parseFilter(Map<String, Object> filter, NamedXContentRegistry registry) throws IOException {
        try (
            XContentBuilder builder = JsonXContent.contentBuilder().map(filter);
            XContentParser parser = XContentHelper.createParser(
                XContentParserConfiguration.EMPTY.withRegistry(registry)
                    .withDeprecationHandler(DeprecationHandler.THROW_UNSUPPORTED_OPERATION),
                BytesReference.bytes(builder),
                XContentType.JSON
            )
        ) {
            return AbstractQueryBuilder.parseTopLevelQuery(parser);
        }
    }

    static void weights(XContentBuilder builder, TrackedQuery.Weights weights) throws IOException {
        builder.field("multiplicity", weights.multiplicity());
        builder.field("weighted_multiplicity", weights.weightedMultiplicity());
        builder.field("inclusion_probability", weights.inclusionProbability());
        builder.field("capture_rate", weights.captureRate());
    }

    static void groundTruth(XContentBuilder builder, GroundTruth groundTruth) throws IOException {
        builder.startObject("ground_truth");
        hits(builder, "neighbors", groundTruth.neighbors());
        if (groundTruth.dataState() != null) {
            builder.startObject("data_state");
            builder.field("documents", groundTruth.dataState().documents());
            builder.field("digest", groundTruth.dataState().digest());
            builder.endObject();
        }
        builder.endObject();
    }

    private static void hits(XContentBuilder builder, String name, Iterable<CapturedSearch.Hit> hits) throws IOException {
        builder.startArray(name);
        for (CapturedSearch.Hit hit : hits) {
            builder.startObject().field("index", hit.index()).field("id", hit.id()).field("score", hit.score()).endObject();
        }
        builder.endArray();
    }
}

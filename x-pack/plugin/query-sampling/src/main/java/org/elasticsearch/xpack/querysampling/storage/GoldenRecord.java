/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Layout of the {@link GoldenIndex} documents: one record per query and one manifest per version.
 */
public final class GoldenRecord {

    private GoldenRecord() {}

    public static String recordId(long version, String fingerprint) {
        return version + "_" + fingerprint;
    }

    public static String manifestId(long version) {
        return "version_" + version;
    }

    /** A copy of the stored sample as it is when promoted, keeping its weights, ground truth and data state. */
    public static XContentBuilder record(XContentBuilder builder, long version, StoredSample sample, long nowMillis) throws IOException {
        CapturedQuery query = sample.search().query();
        builder.startObject();
        builder.field("kind", GoldenIndex.RECORD);
        builder.field("dataset_version", version);
        builder.field("promoted_at", nowMillis);
        builder.field("source_picked_at", sample.pickedAt());
        SampleRecord.identity(
            builder,
            sample.samplerId(),
            sample.fingerprint(),
            null,
            query,
            sample.weights(),
            sample.stratum(),
            sample.hardness(),
            sample.selectivity()
        );
        SampleRecord.query(builder, query);
        SampleRecord.liveHits(builder, sample.search());
        SampleRecord.groundTruth(builder, sample.groundTruth());
        return builder.endObject();
    }

    /**
     * Made first, which gives out the version number, and completed once the records are in.
     *
     * @param sourceVersion the version this one refreshes, null for a version made by promotion
     */
    public static XContentBuilder manifest(
        XContentBuilder builder,
        long version,
        long createdAt,
        long records,
        boolean completed,
        @Nullable Long sourceVersion
    ) throws IOException {
        builder.startObject();
        builder.field("kind", GoldenIndex.VERSION);
        builder.field("dataset_version", version);
        builder.field("created_at", createdAt);
        builder.field("records", records);
        builder.field("completed", completed);
        if (sourceVersion != null) {
            builder.field("source_version", sourceVersion);
        }
        return builder.endObject();
    }

    /** Reads a record back as the stored sample it was copied from; its update time is the promotion time. */
    public static StoredSample parse(Map<String, Object> source, NamedXContentRegistry registry) throws IOException {
        Map<String, Object> asSample = new HashMap<>(source);
        asSample.put("picked_at", source.get("source_picked_at"));
        asSample.put("updated_at", source.get("promoted_at"));
        return SampleRecord.parse(asSample, registry);
    }

    public static XContentBuilder completion(XContentBuilder builder, long records) throws IOException {
        return builder.startObject().field("records", records).field("completed", true).endObject();
    }
}

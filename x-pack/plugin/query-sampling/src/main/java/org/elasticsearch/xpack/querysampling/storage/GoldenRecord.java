/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * How the documents of {@link GoldenIndex} are laid out: the records, one for each query of a version of the dataset, and
 * the manifests, one for each version.
 */
public final class GoldenRecord {

    private GoldenRecord() {}

    /**
     * Identifies the query within a version of the dataset, which has each query once.
     */
    public static String recordId(long version, String fingerprint) {
        return version + "_" + fingerprint;
    }

    public static String manifestId(long version) {
        return "version_" + version;
    }

    /**
     * The document of a query that is promoted to a version of the dataset. It is a copy of the stored sample as it is when it
     * is promoted: the weights say how it was picked, and the ground truth with its data state say what it is worth.
     */
    public static XContentBuilder record(XContentBuilder builder, long version, StoredSample sample, long nowMillis) throws IOException {
        CapturedQuery query = sample.search().query();
        builder.startObject();
        builder.field("kind", GoldenIndex.RECORD);
        builder.field("dataset_version", version);
        builder.field("promoted_at", nowMillis);
        builder.field("source_picked_at", sample.pickedAt());
        builder.field("sampler_id", sample.samplerId());
        builder.field("fingerprint", sample.fingerprint());
        builder.array("indices", query.indices());
        builder.field("field", query.field());
        builder.field("k", query.k());
        SampleRecord.weights(builder, sample.weights());
        if (sample.stratum() != null) {
            builder.field("spatial_space", sample.stratum().space());
            builder.field("spatial_cluster", sample.stratum().cluster());
        }
        if (sample.hardness() != null) {
            builder.field("hardness", sample.hardness().name().toLowerCase(Locale.ROOT));
        }
        if (sample.selectivity() != null) {
            builder.field("selectivity", sample.selectivity().name().toLowerCase(Locale.ROOT));
        }
        SampleRecord.query(builder, query);
        SampleRecord.liveHits(builder, sample.search());
        SampleRecord.groundTruthObject(builder, sample.groundTruth());
        return builder.endObject();
    }

    /**
     * The document that tells a version of the dataset. It is made first, which gives out the number, and completed when the
     * records are in.
     */
    public static XContentBuilder manifest(XContentBuilder builder, long version, long createdAt, long records, boolean completed)
        throws IOException {
        builder.startObject();
        builder.field("kind", GoldenIndex.VERSION);
        builder.field("dataset_version", version);
        builder.field("created_at", createdAt);
        builder.field("records", records);
        builder.field("completed", completed);
        return builder.endObject();
    }

    /**
     * Reads a record back as the stored sample it was copied from, which is what everything that works on queries and their
     * ground truth takes. Its time of being picked is that of the sample, and the time it was written is the promotion.
     *
     * @param registry needed to read the filters of the query
     */
    public static StoredSample parse(Map<String, Object> source, NamedXContentRegistry registry) throws IOException {
        Map<String, Object> asSample = new HashMap<>(source);
        asSample.put("picked_at", source.get("source_picked_at"));
        asSample.put("updated_at", source.get("promoted_at"));
        return SampleRecord.parse(asSample, registry);
    }

    /**
     * What changes in the manifest when the records are in.
     */
    public static XContentBuilder completion(XContentBuilder builder, long records) throws IOException {
        return builder.startObject().field("records", records).field("completed", true).endObject();
    }
}

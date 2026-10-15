/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * The golden dataset: promoted queries with their ground truth, kept for good (nothing deletes from this index).
 * Records of a version are never changed, so a benchmarked version can be found again as it was. A manifest document
 * numbers each version and says whether it is complete; records and manifests are told apart by {@code kind}.
 */
public final class GoldenIndex {

    public static final String NAME = ".query_golden";

    static final int MAPPINGS_VERSION = 2;

    static final String RECORD = "record";
    static final String VERSION = "version";

    private GoldenIndex() {}

    public static SystemIndexDescriptor descriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(NAME + "*")
            .setPrimaryIndex(NAME)
            .setDescription("Contains the golden dataset of kNN queries, with their ground truth, promoted from the sampled queries")
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(QUERY_SAMPLING_ORIGIN)
            .build();
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    // query, live hits and ground truth are only read back whole, so they are not indexed
    static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
            builder.field("dynamic", "strict");
            builder.startObject("_meta");
            builder.field(SystemIndexDescriptor.VERSION_META_KEY, MAPPINGS_VERSION);
            builder.endObject();
            builder.startObject("properties");
            field(builder, "kind", "keyword");
            field(builder, "dataset_version", "long");
            // of a manifest
            field(builder, "created_at", "date");
            field(builder, "records", "long");
            field(builder, "completed", "boolean");
            field(builder, "source_version", "long");
            // of a record
            field(builder, "promoted_at", "date");
            field(builder, "source_picked_at", "date");
            field(builder, "sampler_id", "keyword");
            field(builder, "fingerprint", "keyword");
            field(builder, "indices", "keyword");
            field(builder, "field", "keyword");
            field(builder, "k", "integer");
            field(builder, "multiplicity", "long");
            field(builder, "weighted_multiplicity", "double");
            field(builder, "inclusion_probability", "double");
            field(builder, "seen_probability", "double");
            field(builder, "capture_rate", "double");
            field(builder, "spatial_space", "keyword");
            field(builder, "spatial_cluster", "integer");
            field(builder, "hardness", "keyword");
            field(builder, "selectivity", "keyword");
            notIndexed(builder, "query");
            notIndexed(builder, "live_hits");
            notIndexed(builder, "ground_truth");
            builder.endObject();
            builder.endObject();
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to build the mappings of " + NAME, e);
        }
    }

    private static void field(XContentBuilder builder, String name, String type) throws IOException {
        builder.startObject(name).field("type", type).endObject();
    }

    private static void notIndexed(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "object").field("enabled", false).endObject();
    }
}

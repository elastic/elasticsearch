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
 * Durable home of the sampled queries, one document per query. A document belongs to one sampler run because its
 * weights only make sense against that sampler's counts; documents of different samplers are combined on read.
 */
public final class QuerySamplingIndex {

    public static final String NAME = ".query_sampling";

    static final int MAPPINGS_VERSION = 4;

    private QuerySamplingIndex() {}

    public static SystemIndexDescriptor descriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(NAME + "*")
            .setPrimaryIndex(NAME)
            .setDescription("Contains the sampled kNN queries used to estimate the quality of search")
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

    // query, live hits and ground truth are only read back whole, so they stay in the source and are not indexed
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
            keyword(builder, "sampler_id");
            keyword(builder, "fingerprint");
            keyword(builder, "event_id");
            keyword(builder, "indices");
            keyword(builder, "field");
            field(builder, "k", "integer");
            field(builder, "multiplicity", "long");
            field(builder, "weighted_multiplicity", "double");
            field(builder, "inclusion_probability", "double");
            field(builder, "seen_probability", "double");
            field(builder, "capture_rate", "double");
            keyword(builder, "spatial_space");
            field(builder, "spatial_cluster", "integer");
            keyword(builder, "hardness");
            keyword(builder, "selectivity");
            field(builder, "picked_at", "date");
            field(builder, "updated_at", "date");
            field(builder, "has_ground_truth", "boolean");
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

    private static void keyword(XContentBuilder builder, String name) throws IOException {
        field(builder, name, "keyword");
    }

    private static void field(XContentBuilder builder, String name, String type) throws IOException {
        builder.startObject(name).field("type", type).endObject();
    }

    private static void notIndexed(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "object").field("enabled", false).endObject();
    }
}

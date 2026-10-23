/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * What {@link QuerySamplingIndex} and {@link GoldenIndex} have in common: both are small strict system indices whose
 * documents are shaped like a sampled query.
 */
final class IndexDefinitions {

    private IndexDefinitions() {}

    static SystemIndexDescriptor descriptor(String name, String description, XContentBuilder mappings) {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(name + "*")
            .setPrimaryIndex(name)
            .setDescription(description)
            .setMappings(mappings)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .build()
            )
            .setOrigin(QUERY_SAMPLING_ORIGIN)
            .build();
    }

    static XContentBuilder mappings(String name, int version, CheckedConsumer<XContentBuilder, IOException> properties) {
        try {
            XContentBuilder builder = jsonBuilder();
            builder.startObject();
            builder.startObject(SINGLE_MAPPING_NAME);
            builder.field("dynamic", "strict");
            builder.startObject("_meta");
            builder.field(SystemIndexDescriptor.VERSION_META_KEY, version);
            builder.endObject();
            builder.startObject("properties");
            properties.accept(builder);
            builder.endObject();
            builder.endObject();
            builder.endObject();
            return builder;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to build the mappings of " + name, e);
        }
    }

    /** The properties of {@link SampleRecord#identity}. */
    static void identityProperties(XContentBuilder builder, boolean withEventId) throws IOException {
        field(builder, "sampler_id", "keyword");
        field(builder, "fingerprint", "keyword");
        if (withEventId) {
            field(builder, "event_id", "keyword");
        }
        field(builder, "indices", "keyword");
        field(builder, "field", "keyword");
        field(builder, "k", "integer");
        field(builder, "multiplicity", "long");
        field(builder, "weighted_multiplicity", "double");
        field(builder, "inclusion_probability", "double");
        field(builder, "capture_rate", "double");
        field(builder, "spatial_space", "keyword");
        field(builder, "spatial_cluster", "integer");
        field(builder, "hardness", "keyword");
        field(builder, "selectivity", "keyword");
    }

    /** The query, live hits and ground truth are only read back whole, so they stay in the source and are not indexed. */
    static void payloadProperties(XContentBuilder builder) throws IOException {
        notIndexed(builder, "query");
        notIndexed(builder, "live_hits");
        notIndexed(builder, "ground_truth");
    }

    static void field(XContentBuilder builder, String name, String type) throws IOException {
        builder.startObject(name).field("type", type).endObject();
    }

    private static void notIndexed(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "object").field("enabled", false).endObject();
    }
}

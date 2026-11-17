/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ESQL_ORIGIN;

/**
 * System index holding stored ES|QL results: one document per page, with the serialized page in binary doc values, and the
 * result's metadata as page {@code 0}. A result is routed by its id and the index is sorted by result and page, so it sits on
 * one shard, in order. Fields are single-valued doc values only, so that a bulk of pages is indexed as one columnar batch. The
 * index is never refreshed on a schedule.
 */
public final class EsqlStreamResultsIndexDescriptor {

    /**
     * The index is only registered, and results only stored, when this is enabled.
     */
    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("esql_stream_stored_results");

    public static final String INDEX = ".esql-stream-results";

    public static final String STREAM_ID_FIELD = "stream_id";
    public static final String SEQ_FIELD = "seq";
    public static final String OFFSET_FIELD = "offset";
    public static final String SIZE_FIELD = "size";
    public static final String TRANSPORT_VERSION_FIELD = "transport_version";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String DATA_FIELD = "data";

    private static final int MAPPINGS_VERSION = 0;

    /**
     * Spreads the writes of concurrent results over nodes.
     */
    static final int NUMBER_OF_SHARDS = 6;

    private EsqlStreamResultsIndexDescriptor() {}

    /**
     * The id of the document holding page {@code pageSeq} of the result stored under {@code streamId}.
     */
    public static String pageDocId(String streamId, int pageSeq) {
        return streamId + "/" + pageSeq;
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUMBER_OF_SHARDS)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            // only adjacent segments are merged, so a segment holds results written about the same time and is dropped
            // without a merge once they all expire
            .put(
                MergePolicyConfig.INDEX_MERGE_POLICY_TYPE_SETTING.getKey(),
                MergePolicyConfig.Type.TIME_BASED.name().toLowerCase(Locale.ROOT)
            )
            .putList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), STREAM_ID_FIELD, SEQ_FIELD)
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .field(SystemIndexDescriptor.VERSION_META_KEY, MAPPINGS_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject(STREAM_ID_FIELD)
                .field("type", "keyword")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(SEQ_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(OFFSET_FIELD)
                .field("type", "long")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(SIZE_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(TRANSPORT_VERSION_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(DATA_FIELD)
                .field("type", "binary")
                .field("multi_value", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + INDEX, e);
        }
    }

    public static SystemIndexDescriptor getSystemIndexDescriptor() {
        return SystemIndexDescriptor.builder()
            .setIndexPattern(INDEX + "*")
            .setDescription("Streamed ES|QL results")
            .setPrimaryIndex(INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(ESQL_ORIGIN)
            .build();
    }
}

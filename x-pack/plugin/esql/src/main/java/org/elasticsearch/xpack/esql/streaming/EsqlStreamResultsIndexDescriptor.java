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
    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("esql_stream_results_stored");

    public static final String INDEX = ".esql-stream-results";

    public static final String STREAM_ID_FIELD = "stream_id";
    public static final String STREAM_HEADERS = "stream_headers";
    public static final String STREAM_WARNINGS = "stream_warnings";
    public static final String STREAM_COLUMNS = "stream_columns";
    public static final String STREAM_TASK = "stream_task";
    public static final String STREAM_EXPIRATION_TIME_FIELD = "stream_expiration_time";
    public static final String STREAM_STATUS = "stream_status";
    public static final String STREAM_TOTAL_PAGES = "stream_pages";
    public static final String STREAM_TOTAL_ROWS = "stream_rows";
    public static final String STREAM_TOOK = "stream_took";
    public static final String STREAM_PARTIAL = "stream_partial";
    public static final String STREAM_FAILURE = "stream_failure";

    public static final String PAGE_SEQ_FIELD = "seq";
    public static final String PAGE_OFFSET_FIELD = "offset";
    public static final String PAGE_SIZE_FIELD = "size";
    public static final String PAGE_TRANSPORT_VERSION_FIELD = "transport_version";
    public static final String PAGE_FIELD = "page";

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
            .putList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), STREAM_ID_FIELD, PAGE_SEQ_FIELD)
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
                .startObject(STREAM_HEADERS)
                .field("type", "text")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_WARNINGS)
                .field("type", "text")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_COLUMNS)
                .field("type", "text")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_TASK)
                .field("type", "keyword")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_STATUS)
                .field("type", "keyword")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_TOTAL_PAGES)
                .field("type", "long")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_TOTAL_ROWS)
                .field("type", "long")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_TOOK)
                .field("type", "long")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_PARTIAL)
                .field("type", "boolean")
                .field("multi_value", false)
                .endObject()
                .startObject(STREAM_FAILURE)
                .field("type", "text")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_SEQ_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_OFFSET_FIELD)
                .field("type", "long")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_SIZE_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_TRANSPORT_VERSION_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_FIELD)
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
            .setNetNew()
            .build();
    }
}

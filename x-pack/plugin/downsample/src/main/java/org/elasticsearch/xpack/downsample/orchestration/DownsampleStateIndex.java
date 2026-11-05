/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ROLLUP_ORIGIN;

/**
 * The system index that holds the state of the downsample operations when they are orchestrated by the master instead of by
 * a single request plus persistent tasks. It holds two kinds of documents, see {@link IndexTaskDoc} and {@link ShardTaskDoc}.
 * Only {@link DownsampleStateStore} reads and writes it.
 */
public final class DownsampleStateIndex {

    /**
     * Gates the system index orchestration while it is a proof of concept.
     */
    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("downsample_system_index");

    public static final String INDEX_NAME = ".downsample-tasks";
    public static final String INDEX_PATTERN = INDEX_NAME + "*";
    static final int MAPPINGS_VERSION = 1;

    static final String DOC_TYPE_FIELD = "doc_type";
    static final String INDEX_TASK_DOC_TYPE = "index_task";
    static final String SHARD_TASK_DOC_TYPE = "shard_task";

    private DownsampleStateIndex() {}

    public static SystemIndexDescriptor descriptor() {
        return SystemIndexDescriptor.builder()
            .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
            .setIndexPattern(INDEX_PATTERN)
            .setPrimaryIndex(INDEX_NAME)
            .setDescription("Contains the state of downsample operations")
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(ROLLUP_ORIGIN)
            .setNetNew()
            .build();
    }

    public static String indexTaskId(String targetIndex) {
        return "idx|" + targetIndex;
    }

    public static String shardTaskId(String targetIndex, String targetIndexUuid, int shard) {
        return "shard|" + targetIndex + "|" + targetIndexUuid + "|" + shard;
    }

    private static Settings settings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private static XContentBuilder mappings() {
        try {
            XContentBuilder builder = jsonBuilder().startObject().startObject(SINGLE_MAPPING_NAME);
            builder.field("dynamic", "strict");
            builder.startObject("_meta").field(SystemIndexDescriptor.VERSION_META_KEY, MAPPINGS_VERSION).endObject();
            builder.startObject("properties");
            keyword(builder, DOC_TYPE_FIELD);
            // shared by both document types
            keyword(builder, IndexTaskDoc.TARGET_INDEX.getPreferredName());
            keyword(builder, IndexTaskDoc.TARGET_INDEX_UUID.getPreferredName());
            keyword(builder, IndexTaskDoc.SOURCE_INDEX_UUID.getPreferredName());
            integer(builder, IndexTaskDoc.ATTEMPT.getPreferredName());
            notIndexedText(builder, IndexTaskDoc.LAST_ERROR.getPreferredName());
            // index task
            keyword(builder, IndexTaskDoc.SOURCE_INDEX.getPreferredName());
            keyword(builder, IndexTaskDoc.PHASE.getPreferredName());
            disabledObject(builder, IndexTaskDoc.CONFIG.getPreferredName());
            date(builder, IndexTaskDoc.NEXT_RETRY_AT.getPreferredName());
            notIndexedText(builder, IndexTaskDoc.ERROR_HISTORY.getPreferredName());
            date(builder, IndexTaskDoc.CREATED_AT.getPreferredName());
            date(builder, IndexTaskDoc.DEADLINE.getPreferredName());
            // shard task
            integer(builder, ShardTaskDoc.SHARD.getPreferredName());
            disabledObject(builder, ShardTaskDoc.PARAMS.getPreferredName());
            keyword(builder, ShardTaskDoc.STATUS.getPreferredName());
            keyword(builder, ShardTaskDoc.ASSIGNED_NODE.getPreferredName());
            builder.startObject(ShardTaskDoc.EPOCH.getPreferredName()).field("type", "long").endObject();
            builder.startObject(ShardTaskDoc.RETRIABLE.getPreferredName()).field("type", "boolean").endObject();
            keyword(builder, ShardTaskDoc.RUN_ID.getPreferredName());
            disabledObject(builder, ShardTaskDoc.PROGRESS.getPreferredName());
            builder.endObject();
            return builder.endObject().endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + INDEX_NAME + " index mappings", e);
        }
    }

    private static void keyword(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "keyword").endObject();
    }

    private static void integer(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "integer").endObject();
    }

    private static void date(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "date").field("format", "epoch_millis").endObject();
    }

    private static void notIndexedText(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "text").field("index", false).endObject();
    }

    /**
     * Stored in the source only: these objects are read back as a whole and never queried.
     */
    private static void disabledObject(XContentBuilder builder, String name) throws IOException {
        builder.startObject(name).field("type", "object").field("enabled", false).endObject();
    }
}

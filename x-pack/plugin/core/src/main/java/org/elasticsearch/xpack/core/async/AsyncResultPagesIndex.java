/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;

/**
 * System index that holds the pages of async query results, one document per page. The response
 * itself, without its pages, stays in the async results index so that ownership, keep-alive and
 * deletion work as for any other async result.
 * <p>
 * Each document holds a single serialized page in a binary doc-values column so that a result can
 * be read back page by page instead of decoding one large blob. Page {@code 0} of a result holds its
 * metadata. Fields are single-valued and only stored in doc values, so a bulk of pages can be indexed as one
 * columnar batch. The index is sorted by result and page so that the
 * pages of a result are contiguous and in order. The index is never refreshed on a schedule: writers
 * refresh it once the pages they wrote must become visible.
 */
public final class AsyncResultPagesIndex {

    public static final String INDEX = ".async-result-pages";

    public static final String ASYNC_ID_FIELD = "async_id";
    public static final String PAGE_SEQ_FIELD = "page_seq";
    public static final String FIRST_ROW_FIELD = "first_row";
    public static final String ROW_COUNT_FIELD = "row_count";
    public static final String TRANSPORT_VERSION_FIELD = "transport_version";
    public static final String EXPIRATION_TIME_FIELD = "expiration_time";
    public static final String PAGE_FIELD = "page";

    private static final int MAPPINGS_VERSION = 0;

    private AsyncResultPagesIndex() {}

    /**
     * The id of the document holding page {@code pageSeq} of the result stored under {@code asyncDocId}.
     */
    public static String pageDocId(String asyncDocId, int pageSeq) {
        return asyncDocId + "/" + pageSeq;
    }

    static Settings settings() {
        return Settings.builder()
            .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), "-1")
            .putList(IndexSortConfig.INDEX_SORT_FIELD_SETTING.getKey(), ASYNC_ID_FIELD, PAGE_SEQ_FIELD)
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
                .startObject(ASYNC_ID_FIELD)
                .field("type", "keyword")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(PAGE_SEQ_FIELD)
                .field("type", "integer")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(FIRST_ROW_FIELD)
                .field("type", "long")
                .field("index", false)
                .field("multi_value", false)
                .endObject()
                .startObject(ROW_COUNT_FIELD)
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
            .setDescription("Pages of async query results")
            .setPrimaryIndex(INDEX)
            .setMappings(mappings())
            .setSettings(settings())
            .setOrigin(ASYNC_SEARCH_ORIGIN)
            .build();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentFactory;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Regression test for the false-positive bug in {@code bool{filter: term, must_not: term}} queries
 * on TSDB indices ({@code elastic/elasticsearch#155653}).
 *
 * <p>Root cause: {@code DocValuesRangeIterator.docIDRunEnd()} over-reports run ends for
 * {@code MAYBE} and {@code YES_IF_PRESENT} blocks, causing the bulk scorer to skip per-doc
 * {@code matches()} calls and return false positives. The fix in {@code XDocValuesRangeIterator}
 * returns {@code approximation().docID()} for those cases, limiting the run to the current doc.
 */
public class TsdbBoolMustNotReproductionTests extends ESSingleNodeTestCase {

    /**
     * Indexes 2048 docs into a single TSDB segment so that the mixed {@code dimension} block has
     * {@code YES_IF_PRESENT} status. 2046 docs have {@code dimension=required, label=excluded},
     * one has {@code dimension=required, label=included} (expected hit), and one has
     * {@code dimension=other, label=included} (false positive without the fix).
     */
    public void testBoolMustNotFalsePositive() throws Exception {
        final String index = "tsdb-bool-repro";

        // LRUQueryCache masks the bug by returning cached results from a prior non-DV execution;
        // disable it so every search exercises the DocValuesRangeIterator path directly.
        createIndex(
            index,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dimension")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
                .put("index.queries.cache.enabled", false)
                .build(),
            "@timestamp",
            "type=date",
            "dimension",
            "type=keyword,time_series_dimension=true",
            "label",
            "type=keyword,index=false"
        );

        final long baseTs = 1704067200000L; // 2024-01-01T00:00:00Z
        for (int i = 0; i < 2046; i++) {
            prepareIndex(index).setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("@timestamp", baseTs + i)
                    .field("dimension", "required")
                    .field("label", "excluded")
                    .endObject()
            ).get();
        }
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", baseTs + 2046)
                .field("dimension", "required")
                .field("label", "included")
                .endObject()
        ).get();
        // This doc must not appear in results (filter: dimension=required does not match).
        // Without the fix, docIDRunEnd() for the YES_IF_PRESENT mixed block returns the block end,
        // so the bulk scorer skips matches() and collects this doc as a false positive.
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", baseTs + 2047)
                .field("dimension", "other")
                .field("label", "included")
                .endObject()
        ).get();

        client().admin().indices().prepareRefresh(index).get();

        assertHitCount(
            client().prepareSearch(index)
                .setQuery(
                    QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery("dimension", "required"))
                        .mustNot(QueryBuilders.termQuery("label", "excluded"))
                ),
            1L
        );
    }
}

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
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * Regression test for the false-positive bug in {@code bool{filter: term, must_not: term}} queries
 * on TSDB indices ({@code elastic/elasticsearch#155653}).
 *
 * <p>Root cause: {@code DocValuesRangeIterator.docIDRunEnd()} over-reported run ends for
 * {@code MAYBE} and {@code YES_IF_PRESENT} blocks, causing the bulk scorer to skip per-doc
 * {@code matches()} calls and return false positives. Fixed upstream in {@code apache/lucene#16450},
 * which returns the current doc ID for those cases so the run is limited to that doc.
 */
public class TsdbBoolMustNotReproductionTests extends ESSingleNodeTestCase {

    /**
     * Exercises numeric range queries: {@code termQuery} on a TSDB dimension field is executed as
     * a doc-values numeric range lookup. Indexes 2048 docs so the mixed {@code dimension} block has
     * {@code YES_IF_PRESENT} status. 2046 docs have {@code dimension=required, label=excluded},
     * one has {@code dimension=required, label=included} (expected hit), and one has
     * {@code dimension=other, label=included} (false positive without the fix).
     */
    public void testBoolMustNotWithNumericRangeQuery() throws Exception {
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

    /**
     * Exercises a {@code termsQuery} filter (multi-value) alongside a {@code termQuery} filter and
     * {@code mustNot} on a force-merged single segment. Also checks that {@code profile:true} and
     * {@code profile:false} agree — the original symptom was that profiling bypassed the bulk
     * scorer and returned correct results while the normal path returned false positives.
     */
    public void testBoolMustNotWithTermsQuery() throws Exception {
        final String index = "tsdb-bool-two-filter-repro";

        createIndex(
            index,
            Settings.builder()
                .put(IndexSettings.MODE.getKey(), "time_series")
                .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "dim_a")
                .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2024-01-01T00:00:00Z")
                .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2025-01-01T00:00:00Z")
                .put("index.queries.cache.enabled", false)
                .build(),
            "@timestamp",
            "type=date",
            "dim_a",
            "type=keyword,time_series_dimension=true",
            "dim_b",
            "type=keyword,time_series_dimension=true",
            "tag",
            "type=keyword,index=false"
        );

        long ts = 1704067200000L; // 2024-01-01T00:00:00Z

        // wrong dim_a — excluded by first filter
        for (int i = 0; i < 3000; i++) {
            indexDoc(index, ts++, "other", "match", "alpha");
        }
        // right dim_a + dim_b, but tag matches mustNot — large block that triggers the bug
        for (int i = 0; i < 2500; i++) {
            indexDoc(index, ts++, "target", "match", "excluded");
        }
        // wrong dim_b — excluded by second filter
        for (int i = 0; i < 80; i++) {
            indexDoc(index, ts++, "target", "mismatch", "beta");
        }
        // all conditions satisfied — expected hits
        for (int i = 0; i < 100; i++) {
            indexDoc(index, ts++, "target", "match", "gamma");
        }

        client().admin().indices().prepareRefresh(index).get();
        // Single segment ensures DenseConjunctionBulkScorer is reliably triggered.
        client().admin().indices().prepareForceMerge(index).setMaxNumSegments(1).get();

        var query = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("dim_a", "target"))
            .filter(QueryBuilders.termsQuery("dim_b", "match"))
            .mustNot(QueryBuilders.termQuery("tag", "excluded"));

        long[] profileHits = new long[1];
        assertResponse(
            client().prepareSearch(index).setTrackTotalHits(true).setProfile(true).setQuery(query),
            resp -> profileHits[0] = resp.getHits().getTotalHits().value()
        );

        assertEquals("profile:true should return the correct count", 100L, profileHits[0]);
        // Non-profiling path must agree.
        assertHitCount(client().prepareSearch(index).setTrackTotalHits(true).setQuery(query), profileHits[0]);
    }

    private void indexDoc(String index, long timestampMs, String dimA, String dimB, String tag) throws Exception {
        prepareIndex(index).setSource(
            XContentFactory.jsonBuilder()
                .startObject()
                .field("@timestamp", timestampMs)
                .field("dim_a", dimA)
                .field("dim_b", dimB)
                .field("tag", tag)
                .endObject()
        ).get();
    }
}

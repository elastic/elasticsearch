/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Columnar index mode over a stub segment: the {@code _id} is served from doc values (the row number), and {@code _source} is
 * synthesized from the doc values of the mapped fields, so that a search with hits and a GET need no stored fields.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetColumnarIT extends ESIntegTestCase {

    private static final TimeValue TIMEOUT = TimeValue.timeValueSeconds(60);

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class);
    }

    private void createColumnarParquetIndex() throws Exception {
        ParquetMounts.writeFileAndRegisterRepository(this);
        assertAcked(
            prepareCreate("events").setSettings(
                Settings.builder()
                    .put("index.mode", "columnar")
                    // the stub has no postings for the _id, so the _id mapper must not expect any
                    .put(IndexSettings.ID_FIELD_INDEXED.getKey(), false)
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(ParquetMounts.mountSettings(ParquetMounts.COLUMNAR_FIELDS).build())
            ).setMapping(ParquetMounts.COLUMNAR_MAPPING)
        );
    }

    public void testSearchWithHitsAndGetUseDocValuesOnly() throws Exception {
        internalCluster().startNode();
        createColumnarParquetIndex();
        ensureGreen("events");

        SearchResponse response = prepareSearch("events").setSize(3).execute().actionGet(TIMEOUT);
        try {
            assertEquals(3_500, response.getHits().getTotalHits().value());
            SearchHit[] hits = response.getHits().getHits();
            assertEquals(3, hits.length);
            for (SearchHit hit : hits) {
                logger.info("--> hit id=[{}] source=[{}]", hit.getId(), hit.getSourceAsString());
            }
            assertEquals("0", hits[0].getId());
            // rows 0 and 4 have no note, and every third row has no maybe: a missing value is not in the source
            assertEquals("{\"kind\":\"k0\",\"name\":\"n0\",\"status\":0,\"ts\":0}", hits[0].getSourceAsString());
            assertEquals(
                "{\"kind\":\"k2\",\"maybe\":2,\"name\":\"n2\",\"note\":\"note-2\",\"status\":2,\"ts\":2000}",
                hits[2].getSourceAsString()
            );
            assertEquals("2", hits[2].getId());
        } finally {
            response.decRef();
        }

        GetResponse get = client().prepareGet("events", "5").execute().actionGet(TIMEOUT);
        logger.info("--> GET of row 5: exists=[{}] source=[{}]", get.isExists(), get.getSourceAsString());
        assertTrue(get.isExists());
        assertEquals("{\"kind\":\"k0\",\"maybe\":5,\"name\":\"n5\",\"note\":\"note-5\",\"status\":5,\"ts\":5000}", get.getSourceAsString());
    }

    public void testIdQueriesFindTheRowWithoutPostings() throws Exception {
        internalCluster().startNode();
        createColumnarParquetIndex();
        ensureGreen("events");

        assertHits(QueryBuilders.idsQuery().addIds("5"), "5");
        assertHits(QueryBuilders.termQuery("_id", "3499"), "3499");
        assertHits(QueryBuilders.idsQuery().addIds("7", "1", "3500", "x", "07"), "1", "7");
        assertHits(QueryBuilders.termQuery("_id", "3500"));
        assertHits(QueryBuilders.termsQuery("_id", "10", "11", "12"), "10", "11", "12");
    }

    private void assertHits(org.elasticsearch.index.query.QueryBuilder query, String... expectedIds) {
        SearchResponse response = prepareSearch("events").setQuery(query)
            .setSize(10)
            .addSort("_doc", SortOrder.ASC)
            .execute()
            .actionGet(TIMEOUT);
        try {
            List<String> ids = java.util.Arrays.stream(response.getHits().getHits()).map(SearchHit::getId).toList();
            assertEquals(query.toString(), List.of(expectedIds), ids);
        } finally {
            response.decRef();
        }
    }

    public void testQueriesOnOptionalAndStringFields() throws Exception {
        internalCluster().startNode();
        createColumnarParquetIndex();
        ensureGreen("events");

        // 3 of 4 rows have a note (2625), and 2 of 3 rows have a maybe (3500 - 1167 rows with id % 3 == 0)
        assertCount(QueryBuilders.existsQuery("note"), 2_625);
        assertCount(QueryBuilders.existsQuery("maybe"), 2_333);
        // a keyword of a columnar index with no inverted index is found by scanning its binary doc values
        assertCount(QueryBuilders.termQuery("kind", "k1"), 700);
        assertHits(QueryBuilders.termQuery("name", "n1234"), "1234");
        assertCount(QueryBuilders.termQuery("note", "note-3"), 292);
        assertCount(QueryBuilders.rangeQuery("ts").gte(1_000_000).lt(1_010_000), 10);
    }

    private void assertCount(org.elasticsearch.index.query.QueryBuilder query, long expected) {
        SearchResponse response = prepareSearch("events").setQuery(query).setSize(0).setTrackTotalHits(true).execute().actionGet(TIMEOUT);
        try {
            assertEquals(query.toString(), expected, response.getHits().getTotalHits().value());
        } finally {
            response.decRef();
        }
    }
}

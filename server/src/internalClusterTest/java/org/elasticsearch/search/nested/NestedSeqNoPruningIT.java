/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.nested;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertRetentionLeasesAdvanced;
import static org.elasticsearch.index.seqno.SequenceNumbersTestUtils.assertShardsHaveSeqNoDocValues;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that nested indexing and queries continue to work correctly after a force merge
 * that removes sequence number doc values from merged segments.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class NestedSeqNoPruningIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(InternalSettingsPlugin.class);
    }

    public void testNestedQueriesAfterSeqNoPruningMerge() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final var indexName = "test-nested-seqno-pruning";

        prepareCreate(indexName).setSettings(
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
        ).setMapping("""
            {
                "properties": {
                    "category": { "type": "keyword" },
                    "nested_obj": {
                        "type": "nested",
                        "properties": {
                            "tag": { "type": "keyword" },
                            "value": { "type": "integer" }
                        }
                    }
                }
            }""").get();
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(3, 5);
        final int docsPerBatch = randomIntBetween(5, 15);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                int globalDoc = batch * docsPerBatch + doc;
                bulk.add(prepareIndex(indexName).setSource(Strings.format("""
                    {
                        "category": "cat-%d",
                        "nested_obj": [
                            { "tag": "alpha", "value": %d },
                            { "tag": "beta", "value": %d }
                        ]
                    }""", globalDoc % 3, globalDoc, globalDoc + 1000), XContentType.JSON));
            }
            assertNoFailures(bulk.get());
            flush(indexName);
        }

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);
        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        assertRetentionLeasesAdvanced(client(), indexName, totalDocs);

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));
        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );
        refresh(indexName);

        assertShardsHaveSeqNoDocValues(indexName, false, 1);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("nested_obj", termQuery("nested_obj.tag", "alpha"), ScoreMode.None)),
            totalDocs
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("nested_obj", termQuery("nested_obj.tag", "beta"), ScoreMode.None)),
            totalDocs
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("nested_obj", termQuery("nested_obj.tag", "nonexistent"), ScoreMode.None)),
            0L
        );

        assertNoFailuresAndResponse(
            prepareSearch(indexName).setQuery(
                boolQuery().must(termQuery("category", "cat-0"))
                    .must(nestedQuery("nested_obj", termQuery("nested_obj.tag", "alpha"), ScoreMode.Avg))
            ),
            response -> assertThat(response.getHits().getTotalHits().value(), greaterThan(0L))
        );
    }

    public void testMultiLevelNestedAfterSeqNoPruningMerge() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);

        final var indexName = "test-multi-nested-seqno-pruning";

        prepareCreate(indexName).setSettings(
            indexSettings(1, 0).put(IndexSettings.DISABLE_SEQUENCE_NUMBERS.getKey(), true)
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY)
                .put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), "100ms")
        ).setMapping("""
            {
                "properties": {
                    "field": { "type": "keyword" },
                    "level1": {
                        "type": "nested",
                        "properties": {
                            "l1_tag": { "type": "keyword" },
                            "level2": {
                                "type": "nested",
                                "properties": {
                                    "l2_value": { "type": "integer" }
                                }
                            }
                        }
                    }
                }
            }""").get();
        ensureGreen(indexName);

        final int nbBatches = randomIntBetween(3, 5);
        final int docsPerBatch = randomIntBetween(5, 10);
        final long totalDocs = (long) nbBatches * docsPerBatch;

        for (int batch = 0; batch < nbBatches; batch++) {
            var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            for (int doc = 0; doc < docsPerBatch; doc++) {
                int globalDoc = batch * docsPerBatch + doc;
                bulk.add(prepareIndex(indexName).setSource(Strings.format("""
                    {
                        "field": "val-%d",
                        "level1": [
                            {
                                "l1_tag": "x",
                                "level2": [
                                    { "l2_value": %d },
                                    { "l2_value": %d }
                                ]
                            },
                            {
                                "l1_tag": "y",
                                "level2": [
                                    { "l2_value": %d }
                                ]
                            }
                        ]
                    }""", globalDoc, globalDoc, globalDoc + 100, globalDoc + 200), XContentType.JSON));
            }
            assertNoFailures(bulk.get());
            flush(indexName);
        }

        refresh(indexName);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);
        assertShardsHaveSeqNoDocValues(indexName, true, 1);
        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            greaterThan(1L)
        );

        assertRetentionLeasesAdvanced(client(), indexName, totalDocs);

        var forceMerge = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
        assertThat(forceMerge.getFailedShards(), equalTo(0));
        assertThat(
            indicesAdmin().prepareStats(indexName).clear().setSegments(true).get().getPrimaries().getSegments().getCount(),
            equalTo(1L)
        );
        refresh(indexName);

        assertShardsHaveSeqNoDocValues(indexName, false, 1);

        assertHitCount(prepareSearch(indexName).setSize(0).setTrackTotalHits(true), totalDocs);

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("level1", termQuery("level1.l1_tag", "x"), ScoreMode.None)),
            totalDocs
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("level1", termQuery("level1.l1_tag", "y"), ScoreMode.None)),
            totalDocs
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(nestedQuery("level1.level2", termQuery("level1.level2.l2_value", 0), ScoreMode.None)),
            1L
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(
                nestedQuery(
                    "level1",
                    boolQuery().must(termQuery("level1.l1_tag", "x"))
                        .must(nestedQuery("level1.level2", termQuery("level1.level2.l2_value", 0), ScoreMode.None)),
                    ScoreMode.Avg
                )
            ),
            1L
        );

        assertHitCountAndNoFailures(
            prepareSearch(indexName).setQuery(
                nestedQuery(
                    "level1",
                    boolQuery().must(termQuery("level1.l1_tag", "y"))
                        .must(nestedQuery("level1.level2", termQuery("level1.level2.l2_value", 0), ScoreMode.None)),
                    ScoreMode.Avg
                )
            ),
            0L
        );
    }
}

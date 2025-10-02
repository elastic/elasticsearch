/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.TelemetryMetrics;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.ExecutorNames;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.TestRankBuilder;
import org.elasticsearch.search.rank.TestRankShardResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.feature.RankFeatureShardResult;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertScrollResponsesAndHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class CoordinatorSearchPhaseAPMMetricsTests extends ESSingleNodeTestCase {
    private static final String indexName = "test_coordinator_search_phase_metrics";
    private final int num_primaries = randomIntBetween(2, 7);

    private final Set<String> expectedMetrics = Set.of(
        "es.search.coordinator.phases.can_match.duration.histogram",
        "es.search.coordinator.phases.expand.duration.histogram",
        "es.search.coordinator.phases.fetch.duration.histogram",
        "es.search.coordinator.phases.fetch_lookup_fields.duration.histogram",
        "es.search.coordinator.phases.query.duration.histogram"
    );

    private final Set<String> expectedMetricsWithDfs = Set.of(
        "es.search.coordinator.phases.dfs.duration.histogram",
        "es.search.coordinator.phases.dfs_query.duration.histogram",
        "es.search.coordinator.phases.expand.duration.histogram",
        "es.search.coordinator.phases.fetch.duration.histogram",
        "es.search.coordinator.phases.fetch_lookup_fields.duration.histogram"
    );

    private final Set<String> expectedMetricsWithRanking = Set.of(
        "es.search.coordinator.phases.expand.duration.histogram",
        "es.search.coordinator.phases.fetch.duration.histogram",
        "es.search.coordinator.phases.fetch_lookup_fields.duration.histogram",
        "es.search.coordinator.phases.query.duration.histogram",
        "es.search.coordinator.phases.rank_feature.duration.histogram"
    );

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    private void setUpIndex() throws Exception {
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, num_primaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "doc1").setRefreshPolicy(IMMEDIATE).get();
        prepareIndex(indexName).setId("2").setSource("body", "doc2").setRefreshPolicy(IMMEDIATE).get();

        prepareIndex(CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.INDEX_NAME).setId("1")
            .setSource("body", "doc1")
            .setRefreshPolicy(IMMEDIATE)
            .get();
        prepareIndex(CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.INDEX_NAME).setId("2")
            .setSource("body", "doc2")
            .setRefreshPolicy(IMMEDIATE)
            .get();
    }

    @After
    private void afterTest() {
        resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class, CoordinatorSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.class);
    }

    public void testMetricsDfsQueryThenFetch() throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName).setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        var coordinatorMetrics = filterForCoordinatorMetrics(getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, equalTo(expectedMetricsWithDfs));
        assertMeasurements(expectedMetricsWithDfs);
    }

    public void testSearchTransportMetricsQueryThenFetch() throws InterruptedException {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreFilterShardSize(1)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1"
        );

        var coordinatorMetrics = filterForCoordinatorMetrics(getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, equalTo(expectedMetrics));
        assertMeasurements(expectedMetrics);
    }

    public void testSearchMultipleIndices() {
        assertSearchHitsWithoutFailures(
            client().prepareSearch(indexName, ShardSearchPhaseAPMMetricsTests.TestSystemIndexPlugin.INDEX_NAME)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setPreFilterShardSize(1)
                .setQuery(simpleQueryStringQuery("doc1")),
            "1",
            "1"
        );
        var coordinatorMetrics = filterForCoordinatorMetrics(getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, equalTo(expectedMetrics));
        assertMeasurements(expectedMetrics);
    }

    public void testSearchTransportMetricsScroll() {
        assertScrollResponsesAndHitCount(
            client(),
            TimeValue.timeValueSeconds(60),
            client().prepareSearch(indexName).setSize(1).setPreFilterShardSize(1).setQuery(simpleQueryStringQuery("doc1 doc2")),
            2,
            (respNum, response) -> {}
        );
        var coordinatorMetrics = filterForCoordinatorMetrics(getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, equalTo(expectedMetrics));
        assertMeasurements(expectedMetrics);

    }

    public void testSearchRanking() {
        final String indexName = "index";
        final String rankFeatureFieldName = "field";
        final String searchFieldName = "search_field";
        final String searchFieldValue = "some_value";
        final String fetchFieldName = "fetch_field";
        final String fetchFieldValue = "fetch_value";

        final int minDocs = 3;
        final int maxDocs = 10;
        int numDocs = between(minDocs, maxDocs);
        createIndex(indexName);
        // index some documents
        for (int i = 0; i < numDocs; i++) {
            prepareIndex(indexName).setId(String.valueOf(i))
                .setSource(
                    rankFeatureFieldName,
                    "aardvark_" + i,
                    searchFieldName,
                    searchFieldValue,
                    fetchFieldName,
                    fetchFieldValue + "_" + i
                )
                .get();
        }
        indicesAdmin().prepareRefresh(indexName).get();

        var response = client().prepareSearch(indexName)
            .setSource(
                new SearchSourceBuilder().query(new TermQueryBuilder(searchFieldName, searchFieldValue))
                    .size(2)
                    .from(2)
                    .fetchField(fetchFieldName)
                    .rankBuilder(new TestRankBuilder(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {

                        // no need for more than one queries
                        @Override
                        public boolean isCompoundBuilder() {
                            return false;
                        }

                        @Override
                        public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(
                            int size,
                            int from,
                            Client client
                        ) {
                            return new RankFeaturePhaseRankCoordinatorContext(size, from, DEFAULT_RANK_WINDOW_SIZE, false) {
                                @Override
                                protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
                                    float[] scores = new float[featureDocs.length];
                                    for (int i = 0; i < featureDocs.length; i++) {
                                        scores[i] = featureDocs[i].score;
                                    }
                                    scoreListener.onResponse(scores);
                                }
                            };
                        }

                        @Override
                        public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
                            return new QueryPhaseRankCoordinatorContext(RankBuilder.DEFAULT_RANK_WINDOW_SIZE) {
                                @Override
                                public ScoreDoc[] rankQueryPhaseResults(
                                    List<QuerySearchResult> querySearchResults,
                                    SearchPhaseController.TopDocsStats topDocStats
                                ) {
                                    List<RankDoc> rankDocs = new ArrayList<>();
                                    for (int i = 0; i < querySearchResults.size(); i++) {
                                        QuerySearchResult querySearchResult = querySearchResults.get(i);
                                        TestRankShardResult shardResult = (TestRankShardResult) querySearchResult.getRankShardResult();
                                        for (RankDoc trd : shardResult.testRankDocs) {
                                            trd.shardIndex = i;
                                            rankDocs.add(trd);
                                        }
                                    }
                                    rankDocs.sort(Comparator.comparing((RankDoc doc) -> doc.score).reversed());
                                    RankDoc[] topResults = rankDocs.stream().limit(rankWindowSize).toArray(RankDoc[]::new);
                                    topDocStats.fetchHits = topResults.length;
                                    return topResults;
                                }
                            };
                        }

                        @Override
                        public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
                            return new QueryPhaseRankShardContext(queries, from) {

                                @Override
                                public int rankWindowSize() {
                                    return DEFAULT_RANK_WINDOW_SIZE;
                                }

                                @Override
                                public RankShardResult combineQueryPhaseResults(List<TopDocs> rankResults) {
                                    // we know we have just 1 query, so return all the docs from it
                                    return new TestRankShardResult(
                                        Arrays.stream(rankResults.getFirst().scoreDocs)
                                            .map(x -> new RankDoc(x.doc, x.score, x.shardIndex))
                                            .limit(rankWindowSize())
                                            .toArray(RankDoc[]::new)
                                    );
                                }
                            };
                        }

                        @Override
                        public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
                            return new RankFeaturePhaseRankShardContext(rankFeatureFieldName) {
                                @Override
                                public RankShardResult buildRankFeatureShardResult(SearchHits hits, int shardId) {
                                    RankFeatureDoc[] rankFeatureDocs = new RankFeatureDoc[hits.getHits().length];
                                    for (int i = 0; i < hits.getHits().length; i++) {
                                        SearchHit hit = hits.getHits()[i];
                                        rankFeatureDocs[i] = new RankFeatureDoc(hit.docId(), hit.getScore(), shardId);
                                        rankFeatureDocs[i].featureData(parseFeatureData(hit, rankFeatureFieldName));
                                        rankFeatureDocs[i].score = randomFloat();
                                        rankFeatureDocs[i].rank = i + 1;
                                    }
                                    return new RankFeatureShardResult(rankFeatureDocs);
                                }
                            };
                        }
                    })
            )
            .get();
        assertNoFailures(response);
        var coordinatorMetrics = filterForCoordinatorMetrics(getTestTelemetryPlugin().getRegisteredMetrics(InstrumentType.LONG_HISTOGRAM));
        assertThat(coordinatorMetrics, equalTo(expectedMetricsWithRanking));
        assertMeasurements(expectedMetricsWithRanking);
    }

    private List<String> parseFeatureData(SearchHit hit, String fieldName) {
        Object fieldValue = hit.getFields().get(fieldName).getValue();
        @SuppressWarnings("unchecked")
        List<String> fieldValues = fieldValue instanceof List ? (List<String>) fieldValue : List.of(String.valueOf(fieldValue));
        return fieldValues;
    }

    private void resetMeter() {
        getTestTelemetryPlugin().resetMeter();
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }

    private Set<String> filterForCoordinatorMetrics(List<String> registeredMetrics) {
        return registeredMetrics.stream().filter(m -> m.startsWith("es.search.coordinator")).collect(Collectors.toSet());
    }

    private void assertMeasurements(Collection<String> metricNames) {
        for (var metricName : metricNames) {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(metricName);
            assertThat(measurements, hasSize(1));
            assertThat(measurements.getFirst().getLong(), greaterThanOrEqualTo(0L));
        }
    }

    public static class TestSystemIndexPlugin extends Plugin implements SystemIndexPlugin {

        static final String INDEX_NAME = ".test-system-index";

        public TestSystemIndexPlugin() {}

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(
                SystemIndexDescriptor.builder()
                    .setIndexPattern(INDEX_NAME + "*")
                    .setPrimaryIndex(INDEX_NAME)
                    .setSettings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .setMappings("""
                          {
                            "_meta": {
                              "version": "8.0.0",
                              "managed_index_mappings_version": 3
                            },
                            "properties": {
                              "body": { "type": "keyword" }
                            }
                          }
                        """)
                    .setThreadPools(ExecutorNames.DEFAULT_SYSTEM_INDEX_THREAD_POOLS)
                    .setOrigin(ShardSearchPhaseAPMMetricsTests.class.getSimpleName())
                    .build()
            );
        }

        @Override
        public String getFeatureName() {
            return ShardSearchPhaseAPMMetricsTests.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
        }
    }

}

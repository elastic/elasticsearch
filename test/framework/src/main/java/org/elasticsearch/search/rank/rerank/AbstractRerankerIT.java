/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank.rerank;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.constantScoreQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasRank;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * this base class acts as a wrapper for testing different rerankers, and their behavior when exceptions are thrown
 * the main idea is that we:
 *  - index some documents, with a rank feature field and a search field
 *  - have a random initial scoring
 *  - rerank the results based on the rank feature field (converting String -> Float)
 *  - assert that the results are correctly reranked and that we properly close all resources
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 3)
public abstract class AbstractRerankerIT extends ESIntegTestCase {

    public enum ThrowingRankBuilderType {
        THROWING_QUERY_PHASE_SHARD_CONTEXT,
        THROWING_QUERY_PHASE_COORDINATOR_CONTEXT,
        THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT,
        THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT;
    }

    protected abstract RankBuilder getRankBuilder(int rankWindowSize, String rankFeatureField);

    protected abstract RankBuilder getThrowingRankBuilder(int rankWindowSize, String rankFeatureField, ThrowingRankBuilderType type);

    protected abstract Collection<Class<? extends Plugin>> pluginsNeeded();

    protected boolean shouldCheckScores() {
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginsNeeded();
    }

    public void testRerankerNoExceptions() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(getRankBuilder(rankWindowSize, rankFeatureField))
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10),
            response -> {
                assertHitCount(response, 5L);
                int rank = 1;
                for (SearchHit searchHit : response.getHits().getHits()) {
                    assertThat(searchHit, hasId(String.valueOf(5 - (rank - 1))));
                    assertThat(searchHit, hasRank(rank));
                    assertNotNull(searchHit.getFields().get(searchField));
                    if (shouldCheckScores()) {
                        assertEquals(0.5f - ((rank - 1) * 0.1f), searchHit.getScore(), 1e-5f);
                    }
                    rank++;
                }
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testRerankerPagination() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        assertResponse(
            prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(getRankBuilder(rankWindowSize, rankFeatureField))
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(2)
                .setFrom(2),
            response -> {
                assertHitCount(response, 5L);
                int rank = 3;
                for (SearchHit searchHit : response.getHits().getHits()) {
                    assertThat(searchHit, hasId(String.valueOf(5 - (rank - 1))));
                    assertThat(searchHit, hasRank(rank));
                    assertNotNull(searchHit.getFields().get(searchField));
                    if (shouldCheckScores()) {
                        assertEquals(0.5f - ((rank - 1) * 0.1f), searchHit.getScore(), 1e-5f);
                    }
                    rank++;
                }
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testRerankerPaginationOutsideOfBounds() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(getRankBuilder(rankWindowSize, rankFeatureField))
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(2)
                .setFrom(10),
            response -> {
                assertHitCount(response, 5L);
                assertThat(response.getHits().getHits(), emptyArray());
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testNotAllShardsArePresentInFetchPhase() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 10).build());
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A").setRouting("A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B").setRouting("B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C").setRouting("C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D").setRouting("C"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E").setRouting("C")
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(0.1f))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(0.3f))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(0.3f))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(0.3f))
            )
                .setRankBuilder(getRankBuilder(rankWindowSize, rankFeatureField))
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(2),
            response -> {
                assertHitCount(response, 4L);
                assertThat(response.getHits().getHits(), arrayWithSize(2));
                int rank = 1;
                for (SearchHit searchHit : response.getHits().getHits()) {
                    assertThat(searchHit, hasId(String.valueOf(5 - (rank - 1))));
                    assertThat(searchHit, hasRank(rank));
                    assertNotNull(searchHit.getFields().get(searchField));
                    if (shouldCheckScores()) {
                        assertEquals(0.5f - ((rank - 1) * 0.1f), searchHit.getScore(), 1e-5f);
                    }
                    rank++;
                }
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testRerankerNoMatchingDocs() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        assertNoFailuresAndResponse(
            prepareSearch().setQuery(boolQuery().should(constantScoreQuery(matchQuery(searchField, "F")).boost(randomFloat())))
                .setRankBuilder(getRankBuilder(rankWindowSize, rankFeatureField))
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10),
            response -> {
                assertHitCount(response, 0L);
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testQueryPhaseShardThrowingAllShardsFail() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        // this test is irrespective of the number of shards, as we will always reach QueryPhaseRankShardContext#combineQueryPhaseResults
        // even with no results. So, when we get back to the coordinator, all shards will have failed, and the whole response
        // will be marked as a failure
        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        expectThrows(SearchPhaseExecutionException.class, () -> {
            // we split this in two steps, as if the tests fails (i.e. fails to fail) we still want to dec ref and cleanup the response
            // to avoid false positives & polluting other tests
            SearchResponse response = prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(
                    getThrowingRankBuilder(rankWindowSize, rankFeatureField, ThrowingRankBuilderType.THROWING_QUERY_PHASE_SHARD_CONTEXT)
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10)
                .get();
            response.decRef();
        });
        assertNoOpenContext(indexName);
    }

    public void testQueryPhaseCoordinatorThrowingAllShardsFail() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        // when we throw on the coordinator, the onPhaseFailure handler will be invoked, which in turn will mark the whole
        // search request as a failure (i.e. no partial results)

        expectThrows(SearchPhaseExecutionException.class, () -> {
            // we split this in two steps, as if the tests fails (i.e. fails to fail) we still want to dec ref and cleanup the response
            // to avoid false positives & polluting other tests
            SearchResponse response = prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(
                    getThrowingRankBuilder(
                        rankWindowSize,
                        rankFeatureField,
                        ThrowingRankBuilderType.THROWING_QUERY_PHASE_COORDINATOR_CONTEXT
                    )
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10)
                .get();
            response.decRef();
        });
        assertNoOpenContext(indexName);
    }

    public void testRankFeaturePhaseShardThrowingPartialFailures() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 10).build());
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        // we have 10 shards and 5 documents, so when the exception is thrown we know that not all shards will report failures
        assertResponse(
            prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(
                    getThrowingRankBuilder(
                        rankWindowSize,
                        rankFeatureField,
                        ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT
                    )
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10),
            response -> {
                assertThat(response.getFailedShards(), greaterThan(0));
                assertTrue(
                    Arrays.stream(response.getShardFailures())
                        .allMatch(failure -> failure.getCause().getMessage().contains("rfs - simulated failure"))
                );
                assertHitCount(response, 5);
                assertThat(response.getHits().getHits(), emptyArray());
            }
        );
        assertNoOpenContext(indexName);
    }

    public void testRankFeaturePhaseShardThrowingAllShardsFail() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        // we have 1 shard and 5 documents, so when the exception is thrown we know that all shards will have failed
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build());
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        expectThrows(SearchPhaseExecutionException.class, () -> {
            // we split this in two steps, as if the tests fails (i.e. fails to fail) we still want to dec ref and cleanup the response
            // to avoid false positives & polluting other tests
            SearchResponse response = prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(
                    getThrowingRankBuilder(
                        rankWindowSize,
                        rankFeatureField,
                        ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_SHARD_CONTEXT
                    )
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10)
                .get();
            response.decRef();
        });
        assertNoOpenContext(indexName);
    }

    public void testRankFeaturePhaseCoordinatorThrowingAllShardsFail() throws Exception {
        final String indexName = "test_index";
        final String rankFeatureField = "rankFeatureField";
        final String searchField = "searchField";
        final int rankWindowSize = 10;

        createIndex(indexName);
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource(rankFeatureField, 0.1, searchField, "A"),
            prepareIndex(indexName).setId("2").setSource(rankFeatureField, 0.2, searchField, "B"),
            prepareIndex(indexName).setId("3").setSource(rankFeatureField, 0.3, searchField, "C"),
            prepareIndex(indexName).setId("4").setSource(rankFeatureField, 0.4, searchField, "D"),
            prepareIndex(indexName).setId("5").setSource(rankFeatureField, 0.5, searchField, "E")
        );

        expectThrows(SearchPhaseExecutionException.class, () -> {
            // we split this in two steps, as if the tests fails (i.e. fails to fail) we still want to dec ref and cleanup the response
            // to avoid false positives & polluting other tests
            SearchResponse response = prepareSearch().setQuery(
                boolQuery().should(constantScoreQuery(matchQuery(searchField, "A")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "B")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "C")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "D")).boost(randomFloat()))
                    .should(constantScoreQuery(matchQuery(searchField, "E")).boost(randomFloat()))
            )
                .setRankBuilder(
                    getThrowingRankBuilder(
                        rankWindowSize,
                        rankFeatureField,
                        ThrowingRankBuilderType.THROWING_RANK_FEATURE_PHASE_COORDINATOR_CONTEXT
                    )
                )
                .addFetchField(searchField)
                .setTrackTotalHits(true)
                .setAllowPartialSearchResults(true)
                .setSize(10)
                .get();
            response.decRef();
        });
        assertNoOpenContext(indexName);
    }

    protected void assertNoOpenContext(final String indexName) throws Exception {
        assertBusy(
            () -> assertThat(indicesAdmin().prepareStats(indexName).get().getTotal().getSearch().getOpenContexts(), equalTo(0L)),
            1,
            TimeUnit.SECONDS
        );
    }
}

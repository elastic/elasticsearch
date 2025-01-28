/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchHitsTests;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResultsTests;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestTests;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SearchResponseTests extends ESTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    static {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(SuggestTests.getDefaultNamedXContents());
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables()
    );

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private SearchResponse createTestItem(ShardSearchFailure... shardSearchFailures) {
        return createTestItem(false, shardSearchFailures);
    }

    /**
     * This SearchResponse doesn't include SearchHits, Aggregations, Suggestions, ShardSearchFailures, SearchProfileShardResults
     * to make it possible to only test properties of the SearchResponse itself
     */
    private SearchResponse createMinimalTestItem() {
        return createTestItem(true);
    }

    /**
     * if minimal is set, don't include search hits, aggregations, suggest etc... to make test simpler
     */
    private SearchResponse createTestItem(boolean minimal, ShardSearchFailure... shardSearchFailures) {
        boolean timedOut = randomBoolean();
        Boolean terminatedEarly = randomBoolean() ? null : randomBoolean();
        int numReducePhases = randomIntBetween(1, 10);
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        SearchResponse.Clusters clusters;
        if (minimal) {
            clusters = randomSimpleClusters();
        } else {
            clusters = randomClusters();
        }
        if (minimal == false) {
            SearchHits hits = SearchHitsTests.createTestItem(true, true);
            try {
                Suggest suggest = SuggestTests.createTestItem();
                SearchProfileResults profileResults = SearchProfileResultsTests.createTestItem();
                return new SearchResponse(
                    hits,
                    null,
                    suggest,
                    timedOut,
                    terminatedEarly,
                    profileResults,
                    numReducePhases,
                    null,
                    totalShards,
                    successfulShards,
                    skippedShards,
                    tookInMillis,
                    shardSearchFailures,
                    clusters
                );
            } finally {
                hits.decRef();
            }
        } else {
            return SearchResponseUtils.emptyWithTotalHits(
                null,
                totalShards,
                successfulShards,
                skippedShards,
                tookInMillis,
                shardSearchFailures,
                clusters
            );
        }
    }

    /**
     * This test method is typically called when needing "final" Cluster objects, not intermediate CCS state.
     * So all clusters returned (whether for local only or remote cluster searches are in a final state
     * where all clusters are either successful or skipped.
     * @return Randomly chooses between a simple non-CCS Clusters object (1 cluster total with no details)
     * and a "CCS" Cluster that has multiple clusters with details (underlying Cluster objects)
     */
    static SearchResponse.Clusters randomClusters() {
        if (randomBoolean()) {
            return randomSimpleClusters();
        } else {
            return createCCSClusterObject(3, 2, true, 2, 1, 0, 0, new ShardSearchFailure[0]);
        }
    }

    /**
     * @return non-CCS Clusters - either SearchResponse.Clusters.EMPTY or a Clusters object
     * with total=1, and it is marked as either successful or skipped.
     */
    static SearchResponse.Clusters randomSimpleClusters() {
        if (randomBoolean()) {
            return SearchResponse.Clusters.EMPTY;
        } else {
            int totalClusters = 1;
            int successfulClusters = randomIntBetween(0, totalClusters);
            int skippedClusters = totalClusters - successfulClusters;
            return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
        }
    }

    static SearchResponse.Clusters createCCSClusterObject(
        int totalClusters,
        int remoteClusters,
        boolean ccsMinimizeRoundtrips,
        int successfulClusters,
        int skippedClusters,
        int partialClusters,
        int failedClusters,
        ShardSearchFailure[] failures
    ) {
        assert successfulClusters + skippedClusters <= totalClusters : "successful + skipped > totalClusters";
        assert totalClusters == remoteClusters || totalClusters - remoteClusters == 1
            : "totalClusters and remoteClusters must be same or total = remote + 1";

        OriginalIndices localIndices = null;
        if (totalClusters > remoteClusters) {
            localIndices = new OriginalIndices(new String[] { "foo", "bar*" }, IndicesOptions.lenientExpand());
        }
        assert remoteClusters > 0 : "CCS Cluster must have at least one remote cluster";
        Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
        for (int i = 0; i < remoteClusters; i++) {
            remoteClusterIndices.put("cluster_" + i, new OriginalIndices(new String[] { "foo", "bar*" }, IndicesOptions.lenientExpand()));
        }

        var clusters = new SearchResponse.Clusters(localIndices, remoteClusterIndices, ccsMinimizeRoundtrips, alias -> false);

        int successful = successfulClusters;
        int skipped = skippedClusters;
        int partial = partialClusters;
        int failed = failedClusters;

        int i = totalClusters > remoteClusters ? -1 : 0;
        for (; i < remoteClusters; i++) {
            SearchResponse.Cluster.Status status;
            int totalShards = 5;
            int successfulShards;
            int skippedShards;
            int failedShards;
            List<ShardSearchFailure> failureList = Arrays.asList(failures);
            TimeValue took = new TimeValue(1000L);
            if (successful > 0) {
                failedShards = 0;
                status = SearchResponse.Cluster.Status.SUCCESSFUL;
                successfulShards = 5;
                skippedShards = 1;
                failureList = Collections.emptyList();
                successful--;
            } else if (partial > 0) {
                status = SearchResponse.Cluster.Status.PARTIAL;
                successfulShards = 4;
                skippedShards = 1;
                failedShards = 1;
                partial--;
            } else if (skipped > 0) {
                status = SearchResponse.Cluster.Status.SKIPPED;
                successfulShards = 0;
                skippedShards = 0;
                failedShards = 5;
                skipped--;
            } else if (failed > 0) {
                status = SearchResponse.Cluster.Status.FAILED;
                successfulShards = 0;
                skippedShards = 0;
                failedShards = 5;
                failed--;
            } else {
                failedShards = 0;
                throw new IllegalStateException("Test setup coding error - should not get here");
            }
            String clusterAlias = "";
            if (i >= 0) {
                clusterAlias = "cluster_" + i;
            }
            SearchResponse.Cluster cluster = clusters.getCluster(clusterAlias);
            List<ShardSearchFailure> finalFailureList = failureList;
            clusters.swapCluster(
                cluster.getClusterAlias(),
                (k, v) -> new SearchResponse.Cluster.Builder(v).setStatus(status)
                    .setTotalShards(totalShards)
                    .setSuccessfulShards(successfulShards)
                    .setSkippedShards(skippedShards)
                    .setFailedShards(failedShards)
                    .setFailures(finalFailureList)
                    .setTook(took)
                    .setTimedOut(false)
                    .build()
            );
        }
        return clusters;
    }

    /**
     * the "_shard/total/failures" section makes it impossible to directly
     * compare xContent, so we omit it here
     */
    public void testFromXContent() throws IOException {
        var response = createTestItem();
        try {
            doFromXContentTestWithRandomFields(response, false);
        } finally {
            response.decRef();
        }
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent. We test this with a "minimal" SearchResponse, adding random
     * fields to SearchHits, Aggregations etc... is tested in their own tests
     */
    public void testFromXContentWithRandomFields() throws IOException {
        var response = createMinimalTestItem();
        try {
            doFromXContentTestWithRandomFields(response, true);
        } finally {
            response.decRef();
        }
    }

    private void doFromXContentTestWithRandomFields(SearchResponse response, boolean addRandomFields) throws IOException {
        XContentType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(
            ChunkedToXContent.wrapAsToXContent(response),
            xcontentType,
            params,
            humanReadable
        );
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xcontentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xcontentType.xContent(), mutated)) {
            SearchResponse parsed = SearchResponseUtils.parseSearchResponse(parser);
            try {
                assertToXContentEquivalent(
                    originalBytes,
                    XContentHelper.toXContent(parsed, xcontentType, params, humanReadable),
                    xcontentType
                );
            } finally {
                parsed.decRef();
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    /**
     * The "_shard/total/failures" section makes if impossible to directly compare xContent, because
     * the failures in the parsed SearchResponse are wrapped in an extra ElasticSearchException on the client side.
     * Because of this, in this special test case we compare the "top level" fields for equality
     * and the subsections xContent equivalence independently
     */
    public void testFromXContentWithFailures() throws IOException {
        int numFailures = randomIntBetween(1, 5);
        ShardSearchFailure[] failures = new ShardSearchFailure[numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = ShardSearchFailureTests.createTestItem(IndexMetadata.INDEX_UUID_NA_VALUE);
        }
        BytesReference originalBytes;
        SearchResponse response = createTestItem(failures);
        XContentType xcontentType = randomFrom(XContentType.values());
        try {
            final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
            originalBytes = toShuffledXContent(ChunkedToXContent.wrapAsToXContent(response), xcontentType, params, randomBoolean());
        } finally {
            response.decRef();
        }
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            SearchResponse parsed = SearchResponseUtils.parseSearchResponse(parser);
            try {
                for (int i = 0; i < parsed.getShardFailures().length; i++) {
                    ShardSearchFailure parsedFailure = parsed.getShardFailures()[i];
                    ShardSearchFailure originalFailure = failures[i];
                    assertEquals(originalFailure.index(), parsedFailure.index());
                    assertEquals(originalFailure.shard(), parsedFailure.shard());
                    assertEquals(originalFailure.shardId(), parsedFailure.shardId());
                    String originalMsg = originalFailure.getCause().getMessage();
                    assertEquals(
                        parsedFailure.getCause().getMessage(),
                        "Elasticsearch exception [type=parsing_exception, reason=" + originalMsg + "]"
                    );
                    String nestedMsg = originalFailure.getCause().getCause().getMessage();
                    assertEquals(
                        parsedFailure.getCause().getCause().getMessage(),
                        "Elasticsearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]"
                    );
                }
                assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
                assertNull(parser.nextToken());
            } finally {
                parsed.decRef();
            }
        }
    }

    public void testToXContent() throws IOException {
        SearchHit hit = new SearchHit(1, "id1");
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };
        var sHits = new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f);
        {
            SearchResponse response = new SearchResponse(
                sHits,
                null,
                null,
                false,
                null,
                null,
                1,
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            try {
                String expectedString = XContentHelper.stripWhitespace("""
                    {
                      "took": 0,
                      "timed_out": false,
                      "_shards": {
                        "total": 0,
                        "successful": 0,
                        "skipped": 0,
                        "failed": 0
                      },
                      "hits": {
                        "total": {
                          "value": 100,
                          "relation": "eq"
                        },
                        "max_score": 1.5,
                        "hits": [ { "_id": "id1", "_score": 2.0 } ]
                      }
                    }""");
                assertEquals(expectedString, Strings.toString(response));
            } finally {
                response.decRef();
            }
        }
        {
            SearchResponse response = new SearchResponse(
                sHits,
                null,
                null,
                false,
                null,
                null,
                1,
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                new SearchResponse.Clusters(5, 3, 2)
            );
            try {
                String expectedString = XContentHelper.stripWhitespace("""
                    {
                      "took": 0,
                      "timed_out": false,
                      "_shards": {
                        "total": 0,
                        "successful": 0,
                        "skipped": 0,
                        "failed": 0
                      },
                      "_clusters": {
                        "total": 5,
                        "successful": 3,
                        "skipped": 2,
                        "running":0,
                        "partial": 0,
                        "failed": 0
                      },
                      "hits": {
                        "total": {
                          "value": 100,
                          "relation": "eq"
                        },
                        "max_score": 1.5,
                        "hits": [ { "_id": "id1", "_score": 2.0 } ]
                      }
                    }""");
                assertEquals(expectedString, Strings.toString(response));
            } finally {
                response.decRef();
            }
        }
        {
            SearchResponse response = new SearchResponse(
                sHits,
                null,
                null,
                false,
                null,
                null,
                1,
                null,
                20,
                9,
                2,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                createCCSClusterObject(
                    4,
                    3,
                    true,
                    1,
                    1,
                    1,
                    1,
                    new ShardSearchFailure[] { new ShardSearchFailure(new IllegalStateException("corrupt index")) }
                )
            );
            try {
                String expectedString = XContentHelper.stripWhitespace("""
                    {
                      "took": 0,
                      "timed_out": false,
                      "_shards": {
                        "total": 20,
                        "successful": 9,
                        "skipped": 2,
                        "failed": 0
                      },
                      "_clusters": {
                        "total": 4,
                        "successful": 1,
                        "skipped": 1,
                        "running":0,
                        "partial": 1,
                        "failed": 1,
                        "details": {
                          "(local)": {
                            "status": "successful",
                            "indices": "foo,bar*",
                            "took": 1000,
                            "timed_out": false,
                            "_shards": {
                              "total": 5,
                              "successful": 5,
                              "skipped": 1,
                              "failed": 0
                            }
                          },
                          "cluster_1": {
                            "status": "skipped",
                            "indices": "foo,bar*",
                            "took": 1000,
                            "timed_out": false,
                            "_shards": {
                              "total": 5,
                              "successful": 0,
                              "skipped": 0,
                              "failed": 5
                            },
                            "failures": [
                              {
                                "shard": -1,
                                "index": null,
                                "reason": {
                                  "type": "illegal_state_exception",
                                  "reason": "corrupt index"
                                }
                              }
                            ]
                          },
                          "cluster_2": {
                            "status": "failed",
                            "indices": "foo,bar*",
                            "took": 1000,
                            "timed_out": false,
                            "_shards": {
                              "total": 5,
                              "successful": 0,
                              "skipped": 0,
                              "failed": 5
                            },
                            "failures": [
                              {
                                "shard": -1,
                                "index": null,
                                "reason": {
                                  "type": "illegal_state_exception",
                                  "reason": "corrupt index"
                                }
                              }
                            ]
                          },
                          "cluster_0": {
                            "status": "partial",
                            "indices": "foo,bar*",
                            "took": 1000,
                            "timed_out": false,
                            "_shards": {
                              "total": 5,
                              "successful": 4,
                              "skipped": 1,
                              "failed": 1
                            },
                            "failures": [
                              {
                                "shard": -1,
                                "index": null,
                                "reason": {
                                  "type": "illegal_state_exception",
                                  "reason": "corrupt index"
                                }
                              }
                            ]
                          }
                        }
                      },
                      "hits": {
                        "total": {
                          "value": 100,
                          "relation": "eq"
                        },
                        "max_score": 1.5,
                        "hits": [
                          {
                            "_id": "id1",
                            "_score": 2.0
                          }
                        ]
                      }
                    }""");
                assertEquals(expectedString, Strings.toString(response));
            } finally {
                response.decRef();
            }
        }
        sHits.decRef();
    }

    public void testSerialization() throws IOException {
        SearchResponse searchResponse = createTestItem(false);
        try {
            SearchResponse deserialized = copyWriteable(
                searchResponse,
                namedWriteableRegistry,
                SearchResponse::new,
                TransportVersion.current()
            );
            try {
                if (searchResponse.getHits().getTotalHits() == null) {
                    assertNull(deserialized.getHits().getTotalHits());
                } else {
                    assertEquals(searchResponse.getHits().getTotalHits().value(), deserialized.getHits().getTotalHits().value());
                    assertEquals(searchResponse.getHits().getTotalHits().relation(), deserialized.getHits().getTotalHits().relation());
                }
                assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
                assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
                assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
                assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
                assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
                assertEquals(searchResponse.getClusters(), deserialized.getClusters());
            } finally {
                deserialized.decRef();
            }
        } finally {
            searchResponse.decRef();
        }
    }

    public void testToXContentEmptyClusters() throws IOException {
        SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        try {
            SearchResponse deserialized = copyWriteable(
                searchResponse,
                namedWriteableRegistry,
                SearchResponse::new,
                TransportVersion.current()
            );
            try {
                XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
                deserialized.getClusters().toXContent(builder, ToXContent.EMPTY_PARAMS);
                assertEquals(0, Strings.toString(builder).length());
            } finally {
                deserialized.decRef();
            }
        } finally {
            searchResponse.decRef();
        }
    }

    public void testClustersHasRemoteCluster() {
        // local cluster search Clusters objects
        assertFalse(SearchResponse.Clusters.EMPTY.hasRemoteClusters());
        assertFalse(new SearchResponse.Clusters(1, 1, 0).hasRemoteClusters());

        // CCS search Cluster objects

        // TODO: this variant of Clusters should not be allowed in a future ticket, but adding to test for now
        assertTrue(new SearchResponse.Clusters(3, 2, 1).hasRemoteClusters());
        {
            Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
            remoteClusterIndices.put("remote1", new OriginalIndices(new String[] { "*" }, IndicesOptions.LENIENT_EXPAND_OPEN));

            var c = new SearchResponse.Clusters(null, remoteClusterIndices, randomBoolean(), alias -> randomBoolean());
            assertTrue(c.hasRemoteClusters());
        }

        {
            OriginalIndices localIndices = new OriginalIndices(new String[] { "foo*" }, IndicesOptions.LENIENT_EXPAND_OPEN);

            Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
            remoteClusterIndices.put("remote1", new OriginalIndices(new String[] { "*" }, IndicesOptions.LENIENT_EXPAND_OPEN));

            var c = new SearchResponse.Clusters(localIndices, remoteClusterIndices, randomBoolean(), alias -> randomBoolean());
            assertTrue(c.hasRemoteClusters());
        }

        {
            OriginalIndices localIndices = new OriginalIndices(new String[] { "foo*" }, IndicesOptions.LENIENT_EXPAND_OPEN);

            Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
            remoteClusterIndices.put("remote1", new OriginalIndices(new String[] { "*" }, IndicesOptions.LENIENT_EXPAND_OPEN));
            remoteClusterIndices.put("remote2", new OriginalIndices(new String[] { "a*" }, IndicesOptions.LENIENT_EXPAND_OPEN));
            remoteClusterIndices.put("remote3", new OriginalIndices(new String[] { "b*" }, IndicesOptions.LENIENT_EXPAND_OPEN));

            var c = new SearchResponse.Clusters(localIndices, remoteClusterIndices, randomBoolean(), alias -> randomBoolean());
            assertTrue(c.hasRemoteClusters());
        }
    }
}

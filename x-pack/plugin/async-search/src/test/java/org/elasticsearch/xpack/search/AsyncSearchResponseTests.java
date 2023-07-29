/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;
import static org.hamcrest.Matchers.equalTo;

public class AsyncSearchResponseTests extends ESTestCase {
    private SearchResponse searchResponse = randomSearchResponse();
    private NamedWriteableRegistry namedWriteableRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
    }

    protected Writeable.Reader<AsyncSearchResponse> instanceReader() {
        return AsyncSearchResponse::new;
    }

    protected AsyncSearchResponse createTestInstance() {
        return randomAsyncSearchResponse(randomSearchId(), searchResponse);
    }

    protected void assertEqualInstances(AsyncSearchResponse expectedInstance, AsyncSearchResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);
        assertEqualResponses(expectedInstance, newInstance);
    }

    public final void testSerialization() throws IOException {
        for (int runs = 0; runs < 10; runs++) {
            AsyncSearchResponse testInstance = createTestInstance();
            assertSerialization(testInstance);
        }
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance) throws IOException {
        return assertSerialization(testInstance, TransportVersion.current());
    }

    protected final AsyncSearchResponse assertSerialization(AsyncSearchResponse testInstance, TransportVersion version) throws IOException {
        AsyncSearchResponse deserializedInstance = copyInstance(testInstance, version);
        assertEqualInstances(testInstance, deserializedInstance);
        return deserializedInstance;
    }

    protected final AsyncSearchResponse copyInstance(AsyncSearchResponse instance) throws IOException {
        return copyInstance(instance, TransportVersion.current());
    }

    protected AsyncSearchResponse copyInstance(AsyncSearchResponse instance, TransportVersion version) throws IOException {
        return copyWriteable(instance, namedWriteableRegistry, instanceReader(), version);
    }

    static AsyncSearchResponse randomAsyncSearchResponse(String searchId, SearchResponse searchResponse) {
        int rand = randomIntBetween(0, 2);
        return switch (rand) {
            case 0 -> new AsyncSearchResponse(searchId, randomBoolean(), randomBoolean(), randomNonNegativeLong(), randomNonNegativeLong());
            case 1 -> new AsyncSearchResponse(
                searchId,
                searchResponse,
                null,
                randomBoolean(),
                randomBoolean(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            case 2 -> new AsyncSearchResponse(
                searchId,
                searchResponse,
                new ScriptException("messageData", new Exception("causeData"), Arrays.asList("stack1", "stack2"), "sourceData", "langData"),
                randomBoolean(),
                randomBoolean(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            default -> throw new AssertionError();
        };
    }

    static SearchResponse randomSearchResponse() {
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, successfulShards);
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.EMPTY_WITH_TOTAL_HITS;
        return new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    static void assertEqualResponses(AsyncSearchResponse expected, AsyncSearchResponse actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.status(), actual.status());
        assertEquals(expected.getFailure() == null, actual.getFailure() == null);
        assertEquals(expected.isRunning(), actual.isRunning());
        assertEquals(expected.isPartial(), actual.isPartial());
        assertEquals(expected.getStartTime(), actual.getStartTime());
        assertEquals(expected.getExpirationTime(), actual.getExpirationTime());
    }

    public void testToXContentWithoutSearchResponse() throws IOException {
        Date date = new Date();
        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse("id", true, true, date.getTime(), date.getTime());

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(Strings.format("""
                {
                  "id" : "id",
                  "is_partial" : true,
                  "is_running" : true,
                  "start_time_in_millis" : %s,
                  "expiration_time_in_millis" : %s
                }""", date.getTime(), date.getTime()), Strings.toString(builder));
        }

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            builder.humanReadable(true);
            asyncSearchResponse.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("human", "true")));
            assertEquals(
                Strings.format(
                    """
                        {
                          "id" : "id",
                          "is_partial" : true,
                          "is_running" : true,
                          "start_time" : "%s",
                          "start_time_in_millis" : %s,
                          "expiration_time" : "%s",
                          "expiration_time_in_millis" : %s
                        }""",
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(date.toInstant()),
                    date.getTime(),
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(date.toInstant()),
                    date.getTime()
                ),
                Strings.toString(builder)
            );
        }
    }

    // completion_time should be present since search has completed
    public void testToXContentWithSearchResponseAfterCompletion() throws IOException {
        boolean isRunning = false;
        long startTimeMillis = 1689352924517L;
        long expirationTimeMillis = 1689784924517L;
        long took = 22968L;
        long expectedCompletionTime = startTimeMillis + took;

        SearchHits hits = SearchHits.EMPTY_WITHOUT_TOTAL_HITS;
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 2);
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            10,
            9,
            1,
            took,
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY
        );

        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(
            "id",
            searchResponse,
            null,
            false,
            isRunning,
            startTimeMillis,
            expirationTimeMillis
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(Strings.format("""
                {
                  "id" : "id",
                  "is_partial" : false,
                  "is_running" : false,
                  "start_time_in_millis" : %s,
                  "expiration_time_in_millis" : %s,
                  "completion_time_in_millis" : %s,
                  "response" : {
                    "took" : %s,
                    "timed_out" : false,
                    "num_reduce_phases" : 2,
                    "_shards" : {
                      "total" : 10,
                      "successful" : 9,
                      "skipped" : 1,
                      "failed" : 0
                    },
                    "hits" : {
                      "max_score" : 0.0,
                      "hits" : [ ]
                    }
                  }
                }""", startTimeMillis, expirationTimeMillis, expectedCompletionTime, took), Strings.toString(builder));
        }

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            builder.humanReadable(true);
            asyncSearchResponse.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("human", "true")));
            assertEquals(
                Strings.format(
                    """
                        {
                          "id" : "id",
                          "is_partial" : false,
                          "is_running" : false,
                          "start_time" : "%s",
                          "start_time_in_millis" : %s,
                          "expiration_time" : "%s",
                          "expiration_time_in_millis" : %s,
                          "completion_time" : "%s",
                          "completion_time_in_millis" : %s,
                          "response" : {
                            "took" : %s,
                            "timed_out" : false,
                            "num_reduce_phases" : 2,
                            "_shards" : {
                              "total" : 10,
                              "successful" : 9,
                              "skipped" : 1,
                              "failed" : 0
                            },
                            "hits" : {
                              "max_score" : 0.0,
                              "hits" : [ ]
                            }
                          }
                        }""",
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(startTimeMillis)),
                    startTimeMillis,
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(expirationTimeMillis)),
                    expirationTimeMillis,
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(expectedCompletionTime)),
                    expectedCompletionTime,
                    took
                ),
                Strings.toString(builder)
            );
        }
    }

    public void testToXContentWithCCSSearchResponseWhileRunning() throws IOException {
        boolean isRunning = true;
        long startTimeMillis = 1689352924517L;
        long expirationTimeMillis = 1689784924517L;
        long took = 22968L;

        SearchHits hits = SearchHits.EMPTY_WITHOUT_TOTAL_HITS;
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 2);

        SearchResponse.Clusters clusters = createCCSClusterObject(3, 3, true);

        SearchResponse searchResponse = new SearchResponse(sections, null, 10, 9, 1, took, new ShardSearchFailure[0], clusters);

        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(
            "id",
            searchResponse,
            null,
            true,
            isRunning,
            startTimeMillis,
            expirationTimeMillis
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(Strings.format("""
                {
                  "id" : "id",
                  "is_partial" : true,
                  "is_running" : true,
                  "start_time_in_millis" : %s,
                  "expiration_time_in_millis" : %s,
                  "response" : {
                    "took" : %s,
                    "timed_out" : false,
                    "num_reduce_phases" : 2,
                    "_shards" : {
                      "total" : 10,
                      "successful" : 9,
                      "skipped" : 1,
                      "failed" : 0
                    },
                    "_clusters" : {
                      "total" : 3,
                      "successful" : 0,
                      "skipped" : 0,
                      "details" : {
                        "cluster_1" : {
                          "status" : "running",
                          "indices" : "foo,bar*",
                          "timed_out" : false
                        },
                        "cluster_2" : {
                          "status" : "running",
                          "indices" : "foo,bar*",
                          "timed_out" : false
                        },
                        "cluster_0" : {
                          "status" : "running",
                          "indices" : "foo,bar*",
                          "timed_out" : false
                        }
                      }
                    },
                    "hits" : {
                      "max_score" : 0.0,
                      "hits" : [ ]
                    }
                  }
                }""", startTimeMillis, expirationTimeMillis, took), Strings.toString(builder));
        }

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            builder.humanReadable(true);
            asyncSearchResponse.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("human", "true")));
            assertEquals(
                Strings.format(
                    """
                        {
                          "id" : "id",
                          "is_partial" : true,
                          "is_running" : true,
                          "start_time" : "%s",
                          "start_time_in_millis" : %s,
                          "expiration_time" : "%s",
                          "expiration_time_in_millis" : %s,
                          "response" : {
                            "took" : %s,
                            "timed_out" : false,
                            "num_reduce_phases" : 2,
                            "_shards" : {
                              "total" : 10,
                              "successful" : 9,
                              "skipped" : 1,
                              "failed" : 0
                            },
                            "_clusters" : {
                              "total" : 3,
                              "successful" : 0,
                              "skipped" : 0,
                              "details" : {
                                "cluster_1" : {
                                  "status" : "running",
                                  "indices" : "foo,bar*",
                                  "timed_out" : false
                                },
                                "cluster_2" : {
                                  "status" : "running",
                                  "indices" : "foo,bar*",
                                  "timed_out" : false
                                },
                                "cluster_0" : {
                                  "status" : "running",
                                  "indices" : "foo,bar*",
                                  "timed_out" : false
                                }
                              }
                            },
                            "hits" : {
                              "max_score" : 0.0,
                              "hits" : [ ]
                            }
                          }
                        }""",
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(startTimeMillis)),
                    startTimeMillis,
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(expirationTimeMillis)),
                    expirationTimeMillis,
                    took
                ),
                Strings.toString(builder)
            );
        }
    }

    // completion_time should be present since search has completed
    public void testToXContentWithCCSSearchResponseAfterCompletion() throws IOException {
        boolean isRunning = false;
        long startTimeMillis = 1689352924517L;
        long expirationTimeMillis = 1689784924517L;
        long took = 22968L;
        long expectedCompletionTime = startTimeMillis + took;

        SearchHits hits = SearchHits.EMPTY_WITHOUT_TOTAL_HITS;
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, true, null, null, 2);
        SearchResponse.Clusters clusters = createCCSClusterObject(4, 3, true, 2, 1, 1);

        SearchResponse.Cluster localCluster = clusters.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        assertThat(localCluster.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        localCluster.setTotalShards(10);
        localCluster.setSuccessfulShards(10);
        localCluster.setSkippedShards(3);
        localCluster.setFailedShards(0);
        localCluster.setTook(11111L);

        SearchResponse.Cluster cluster0 = clusters.getCluster("cluster_0");
        assertThat(cluster0.getStatus(), equalTo(SearchResponse.Cluster.Status.SUCCESSFUL));
        cluster0.setTotalShards(8);
        cluster0.setSuccessfulShards(8);
        cluster0.setSkippedShards(1);
        cluster0.setFailedShards(0);
        cluster0.setTook(7777L);

        SearchResponse.Cluster cluster1 = clusters.getCluster("cluster_1");
        assertThat(cluster1.getStatus(), equalTo(SearchResponse.Cluster.Status.SKIPPED));
        cluster1.setTotalShards(2);
        cluster1.setSuccessfulShards(0);
        cluster1.setSkippedShards(0);
        cluster1.setFailedShards(2);
        cluster1.addFailure(
            new ShardSearchFailure(
                new NullPointerException("NPE details"),
                new SearchShardTarget("nodeId0", new ShardId("foo", UUID.randomUUID().toString(), 0), "cluster_1")
            )
        );
        cluster1.addFailure(
            new ShardSearchFailure(
                new CorruptIndexException("abc", "123"),
                new SearchShardTarget("nodeId0", new ShardId("bar1", UUID.randomUUID().toString(), 0), "cluster_1")
            )
        );

        SearchResponse.Cluster cluster2 = clusters.getCluster("cluster_2");
        assertThat(cluster2.getStatus(), equalTo(SearchResponse.Cluster.Status.PARTIAL));
        cluster2.setTotalShards(8);
        cluster2.setSuccessfulShards(8);
        cluster2.setSkippedShards(0);
        cluster2.setFailedShards(0);
        cluster2.markAsTimedOut();
        cluster2.setTook(17322L);

        SearchResponse searchResponse = new SearchResponse(sections, null, 10, 9, 1, took, new ShardSearchFailure[0], clusters);

        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(
            "id",
            searchResponse,
            null,
            false,
            isRunning,
            startTimeMillis,
            expirationTimeMillis
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(Strings.format("""
                {
                  "id" : "id",
                  "is_partial" : false,
                  "is_running" : false,
                  "start_time_in_millis" : %s,
                  "expiration_time_in_millis" : %s,
                  "completion_time_in_millis" : %s,
                  "response" : {
                    "took" : %s,
                    "timed_out" : true,
                    "num_reduce_phases" : 2,
                    "_shards" : {
                      "total" : 10,
                      "successful" : 9,
                      "skipped" : 1,
                      "failed" : 0
                    },
                    "_clusters" : {
                      "total" : 4,
                      "successful" : 3,
                      "skipped" : 1,
                      "details" : {
                        "(local)" : {
                          "status" : "successful",
                          "indices" : "foo,bar*",
                          "took" : 11111.0,
                          "timed_out" : false,
                          "_shards" : {
                            "total" : 10,
                            "successful" : 10,
                            "skipped" : 3,
                            "failed" : 0
                          }
                        },
                        "cluster_1" : {
                          "status" : "skipped",
                          "indices" : "foo,bar*",
                          "timed_out" : false,
                          "_shards" : {
                            "total" : 2,
                            "successful" : 0,
                            "skipped" : 0,
                            "failed" : 2
                          },
                          "failures" : [
                            {
                              "shard" : 0,
                              "index" : "cluster_1:foo",
                              "node" : "nodeId0",
                              "reason" : {
                                "type" : "null_pointer_exception",
                                "reason" : "NPE details"
                              }
                            },
                            {
                              "shard" : 0,
                              "index" : "cluster_1:bar1",
                              "node" : "nodeId0",
                              "reason" : {
                                "type" : "corrupt_index_exception",
                                "reason" : "abc (resource=123)"
                              }
                            }
                          ]
                        },
                        "cluster_2" : {
                          "status" : "partial",
                          "indices" : "foo,bar*",
                          "took" : 17322.0,
                          "timed_out" : true,
                          "_shards" : {
                            "total" : 8,
                            "successful" : 8,
                            "skipped" : 0,
                            "failed" : 0
                          }
                        },
                        "cluster_0" : {
                          "status" : "successful",
                          "indices" : "foo,bar*",
                          "took" : 7777.0,
                          "timed_out" : false,
                          "_shards" : {
                            "total" : 8,
                            "successful" : 8,
                            "skipped" : 1,
                            "failed" : 0
                          }
                        }
                      }
                    },
                    "hits" : {
                      "max_score" : 0.0,
                      "hits" : [ ]
                    }
                  }
                }""", startTimeMillis, expirationTimeMillis, expectedCompletionTime, took), Strings.toString(builder));
        }
    }

    // completion_time should NOT be present since search is still running
    public void testToXContentWithSearchResponseWhileRunning() throws IOException {
        boolean isRunning = true;
        long startTimeMillis = 1689352924517L;
        long expirationTimeMillis = 1689784924517L;
        long took = 22968L;

        SearchHits hits = SearchHits.EMPTY_WITHOUT_TOTAL_HITS;
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, false, null, null, 2);
        SearchResponse searchResponse = new SearchResponse(
            sections,
            null,
            10,
            9,
            1,
            took,
            new ShardSearchFailure[0],
            SearchResponse.Clusters.EMPTY
        );

        AsyncSearchResponse asyncSearchResponse = new AsyncSearchResponse(
            "id",
            searchResponse,
            null,
            true,
            isRunning,
            startTimeMillis,
            expirationTimeMillis
        );

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            asyncSearchResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals(Strings.format("""
                {
                  "id" : "id",
                  "is_partial" : true,
                  "is_running" : true,
                  "start_time_in_millis" : %s,
                  "expiration_time_in_millis" : %s,
                  "response" : {
                    "took" : %s,
                    "timed_out" : false,
                    "num_reduce_phases" : 2,
                    "_shards" : {
                      "total" : 10,
                      "successful" : 9,
                      "skipped" : 1,
                      "failed" : 0
                    },
                    "hits" : {
                      "max_score" : 0.0,
                      "hits" : [ ]
                    }
                  }
                }""", startTimeMillis, expirationTimeMillis, took), Strings.toString(builder));
        }

        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            builder.humanReadable(true);
            asyncSearchResponse.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("human", "true")));
            assertEquals(
                Strings.format(
                    """
                        {
                          "id" : "id",
                          "is_partial" : true,
                          "is_running" : true,
                          "start_time" : "%s",
                          "start_time_in_millis" : %s,
                          "expiration_time" : "%s",
                          "expiration_time_in_millis" : %s,
                          "response" : {
                            "took" : %s,
                            "timed_out" : false,
                            "num_reduce_phases" : 2,
                            "_shards" : {
                              "total" : 10,
                              "successful" : 9,
                              "skipped" : 1,
                              "failed" : 0
                            },
                            "hits" : {
                              "max_score" : 0.0,
                              "hits" : [ ]
                            }
                          }
                        }""",
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(startTimeMillis)),
                    startTimeMillis,
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(expirationTimeMillis)),
                    expirationTimeMillis,
                    took
                ),
                Strings.toString(builder)
            );
        }
    }

    static SearchResponse.Clusters createCCSClusterObject(int totalClusters, int remoteClusters, boolean ccsMinimizeRoundtrips) {
        OriginalIndices localIndices = null;
        if (totalClusters > remoteClusters) {
            localIndices = new OriginalIndices(new String[] { "foo", "bar*" }, IndicesOptions.lenientExpand());
        }
        assert remoteClusters > 0 : "CCS Cluster must have at least one remote cluster";
        Map<String, OriginalIndices> remoteClusterIndices = new HashMap<>();
        for (int i = 0; i < remoteClusters; i++) {
            remoteClusterIndices.put("cluster_" + i, new OriginalIndices(new String[] { "foo", "bar*" }, IndicesOptions.lenientExpand()));
        }

        return new SearchResponse.Clusters(localIndices, remoteClusterIndices, ccsMinimizeRoundtrips);
    }

    static SearchResponse.Clusters createCCSClusterObject(
        int totalClusters,
        int remoteClusters,
        boolean ccsMinimizeRoundtrips,
        int successfulClusters,
        int skippedClusters,
        int partialClusters
    ) {
        assert successfulClusters + skippedClusters <= totalClusters : "successful + skipped > totalClusters";
        assert totalClusters == remoteClusters || totalClusters - remoteClusters == 1
            : "totalClusters and remoteClusters must be same or total = remote + 1";
        SearchResponse.Clusters clusters = createCCSClusterObject(totalClusters, remoteClusters, ccsMinimizeRoundtrips);

        int successful = successfulClusters;
        int skipped = skippedClusters;
        int partial = partialClusters;
        if (totalClusters > remoteClusters) {
            if (successful > 0) {
                SearchResponse.Cluster local = clusters.getCluster("");
                local.setStatus(SearchResponse.Cluster.Status.SUCCESSFUL);
                successful--;
            } else if (skipped > 0) {
                SearchResponse.Cluster local = clusters.getCluster("");
                local.setStatus(SearchResponse.Cluster.Status.SKIPPED);
                skipped--;
            } else if (partial > 0) {
                SearchResponse.Cluster local = clusters.getCluster("");
                local.setStatus(SearchResponse.Cluster.Status.PARTIAL);
                partial--;
            }
        }

        int numClusters = successful + skipped + partial;

        for (int i = 0; i < numClusters; i++) {
            SearchResponse.Cluster cluster = clusters.getCluster("cluster_" + i);
            if (successful > 0) {
                cluster.setStatus(SearchResponse.Cluster.Status.SUCCESSFUL);
                successful--;
            } else if (skipped > 0) {
                cluster.setStatus(SearchResponse.Cluster.Status.SKIPPED);
                skipped--;
            } else if (partial > 0) {
                cluster.setStatus(SearchResponse.Cluster.Status.PARTIAL);
                partial--;
            } else {
                throw new IllegalStateException("Test setup coding error - should not get here");
            }
        }
        return clusters;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
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
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.core.async.GetAsyncResultRequestTests.randomSearchId;

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
}

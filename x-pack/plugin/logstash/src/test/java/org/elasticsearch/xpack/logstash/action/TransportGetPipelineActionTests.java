/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetPipelineActionTests extends ESTestCase {

    /**
     * Test that an error message is logged on a partial failure of
     * a TransportGetPipelineAction.
     */
    public void testGetPipelineMultipleIDsPartialFailure() throws Exception {
        // Set up a log appender for detecting log messages
        final MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.elasticsearch.xpack.logstash.action.TransportGetPipelineAction",
                Level.INFO,
                "Could not retrieve logstash pipelines with ids: [2]"
            )
        );
        mockLogAppender.start();
        final Logger logger = LogManager.getLogger(TransportGetPipelineAction.class);

        // Set up a MultiGetResponse
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getId()).thenReturn("1");
        when(mockResponse.getSourceAsBytesRef()).thenReturn(mock(BytesReference.class));
        when(mockResponse.isExists()).thenReturn(true);
        MultiGetResponse.Failure failure = mock(MultiGetResponse.Failure.class);
        when(failure.getId()).thenReturn("2");
        MultiGetResponse multiGetResponse = new MultiGetResponse(
            new MultiGetItemResponse[] { new MultiGetItemResponse(mockResponse, null), new MultiGetItemResponse(null, failure) }
        );

        GetPipelineRequest request = new GetPipelineRequest(List.of("1", "2"));

        // Set up an ActionListener for the actual test conditions
        ActionListener<GetPipelineResponse> testActionListener = new ActionListener<>() {
            @Override
            public void onResponse(GetPipelineResponse getPipelineResponse) {
                // check successful pipeline get
                assertThat(getPipelineResponse, is(notNullValue()));
                assertThat(getPipelineResponse.pipelines().size(), equalTo(1));

                // check that failed pipeline get is logged
                mockLogAppender.assertAllExpectationsMatched();
            }

            @Override
            public void onFailure(Exception e) {
                // do nothing
            }
        };

        try (Client client = getMockClient(multiGetResponse)) {
            Loggers.addAppender(logger, mockLogAppender);
            TransportGetPipelineAction action = new TransportGetPipelineAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                client
            );
            action.doExecute(null, request, testActionListener);
        } finally {
            Loggers.removeAppender(logger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

    /**
     * Test that the explicit and wildcard IDs are requested.
     */
    public void testGetPipelinesByExplicitAndWildcardIds() {
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(prepareSearchHits(), null, null, null, false, null, 1);
        SearchResponse searchResponse = new SearchResponse(
            internalSearchResponse,
            null,
            1,
            1,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY,
            null
        );

        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(prepareSearchHits());

        GetPipelineRequest request = new GetPipelineRequest(List.of("1", "2", "3*"));
        AtomicReference<Exception> failure = new AtomicReference<>();

        // Set up an ActionListener for the actual test conditions
        ActionListener<GetPipelineResponse> testActionListener = new ActionListener<>() {
            @Override
            public void onResponse(GetPipelineResponse getPipelineResponse) {
                assertThat(getPipelineResponse, is(notNullValue()));
                assertThat(getPipelineResponse.pipelines().size(), equalTo(3));
                assertTrue(getPipelineResponse.pipelines().containsKey("1"));
                assertTrue(getPipelineResponse.pipelines().containsKey("2"));
                assertTrue(getPipelineResponse.pipelines().containsKey("3*"));
            }

            @Override
            public void onFailure(Exception e) {
                failure.set(e);
            }
        };

        try (Client client = getMockClient(searchResponse)) {
            new TransportGetPipelineAction(mock(TransportService.class), mock(ActionFilters.class), client).doExecute(
                null,
                request,
                testActionListener
            );
        }

        assertNull(failure.get());
    }

    public void testMissingIndexHandling() throws Exception {
        try (Client failureClient = getFailureClient(new IndexNotFoundException("foo"))) {
            final TransportGetPipelineAction action = new TransportGetPipelineAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                failureClient
            );
            final List<String> pipelines = randomList(0, 10, () -> randomAlphaOfLengthBetween(1, 8));
            final GetPipelineRequest request = new GetPipelineRequest(pipelines);
            PlainActionFuture<GetPipelineResponse> future = new PlainActionFuture<>();
            action.doExecute(null, request, future);
            assertThat(future.get().pipelines(), anEmptyMap());
        }
    }

    private Client getMockClient(ActionResponse response) {
        return new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) response);
            }
        };
    }

    private Client getFailureClient(Exception e) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (randomBoolean()) {
                    listener.onFailure(new RemoteTransportException("failed on other node", e));
                } else {
                    listener.onFailure(e);
                }
            }
        };
    }

    private SearchHits prepareSearchHits() {
        SearchHit hit1 = new SearchHit(0, "1");
        hit1.score(1f);
        hit1.shard(new SearchShardTarget("a", new ShardId("a", "indexUUID", 0), null));

        SearchHit hit2 = new SearchHit(0, "2");
        hit2.score(1f);
        hit2.shard(new SearchShardTarget("a", new ShardId("a", "indexUUID", 0), null));

        SearchHit hit3 = new SearchHit(0, "3*");
        hit3.score(1f);
        hit3.shard(new SearchShardTarget("a", new ShardId("a", "indexUUID", 0), null));

        return new SearchHits(new SearchHit[] { hit1, hit2, hit3 }, new TotalHits(3L, TotalHits.Relation.EQUAL_TO), 1f);
    }
}

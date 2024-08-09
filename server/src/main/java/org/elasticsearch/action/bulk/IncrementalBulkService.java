/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IncrementalBulkService {

    private final boolean allowExplicitIndex;
    private final Client client;

    public IncrementalBulkService(Settings settings, Client client) {
        this.allowExplicitIndex = BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
        this.client = client;
    }

    public BaseRestHandler.RequestBodyChunkConsumer newBulkRequest(RestRequest request, ActionListener<BulkResponse> listener) {
        return new BulkChunkHandler(request, listener);
    }

    private class BulkChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

        private final RestRequest request;
        private final ActionListener<BulkResponse> listener;

        private final Map<String, String> stringDeduplicator = new HashMap<>();
        private final String defaultIndex;
        private final String defaultRouting;
        private final FetchSourceContext defaultFetchSourceContext;
        private final String defaultPipeline;
        private final boolean defaultListExecutedPipelines;
        private final String waitForActiveShards;
        private final Boolean defaultRequireAlias;
        private final boolean defaultRequireDataStream;
        private final TimeValue timeout;
        private final String refresh;
        private final BulkRequestParser parser;

        private volatile RestChannel restChannel;
        private volatile boolean isDone = false;
        private BulkRequest bulkRequest = null;
        private int offset = 0;
        private final ArrayList<ReleasableBytesReference> parsedChunks = new ArrayList<>(4);
        private final ArrayDeque<ReleasableBytesReference> unParsedChunks = new ArrayDeque<>(4);
        private final ArrayList<BulkResponse> responses = new ArrayList<>(2);

        private BulkChunkHandler(RestRequest request, ActionListener<BulkResponse> listener) {
            this.request = request;
            this.defaultIndex = request.param("index");
            this.defaultRouting = request.param("routing");
            this.defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            this.defaultPipeline = request.param("pipeline");
            this.defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
            this.waitForActiveShards = request.param("wait_for_active_shards");
            this.defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
            this.defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
            this.timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
            this.refresh = request.param("refresh");
            this.parser = new BulkRequestParser(true, request.getRestApiVersion());
            this.listener = listener;

            createNewBulkRequest();
        }

        private void createNewBulkRequest() {
            assert bulkRequest == null;
            bulkRequest = new BulkRequest();

            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }
            bulkRequest.timeout(timeout);
            bulkRequest.setRefreshPolicy(refresh);
        }

        @Override
        public void accept(RestChannel restChannel) throws Exception {
            this.restChannel = restChannel;
        }

        // TODO: Work out refCount(). I think we need to retain 1 reference to every chunk passed to us as we use retained slice in the
        // parser.
        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            assert channel == restChannel;

            try {
                // TODO: Check that the behavior here vs. globalRouting, globalPipeline, globalRequireAlias, globalRequireDatsStream in
                // BulkRequest#add is fine

                unParsedChunks.add(chunk);

                final BytesReference data;
                if (unParsedChunks.size() > 1) {
                    BytesReference[] bytesReferences = new BytesReference[unParsedChunks.size()];
                    int i = 0;
                    for (BytesReference bytesReference : unParsedChunks) {
                        if (i == 0) {
                            bytesReferences[i++] = bytesReference.slice(offset, bytesReference.length() - offset);
                        } else {
                            bytesReferences[i++] = bytesReference;
                        }
                    }
                    data = CompositeBytesReference.of(bytesReferences);
                } else {
                    assert offset == 0;
                    data = chunk;
                }

                int bytesConsumed = parser.incrementalParse(
                    data,
                    defaultIndex,
                    defaultRouting,
                    defaultFetchSourceContext,
                    defaultPipeline,
                    defaultRequireAlias,
                    defaultRequireDataStream,
                    defaultListExecutedPipelines,
                    allowExplicitIndex,
                    request.getXContentType(),
                    (request, type) -> bulkRequest.add(request),
                    bulkRequest::internalAdd,
                    bulkRequest::add,
                    stringDeduplicator
                );

                accountParsing(bytesConsumed);

            } catch (IOException e) {
                // TODO: Exception Handling
                throw new UncheckedIOException(e);
            }

            if (isLast) {
                isDone = true;
                assert unParsedChunks.isEmpty();

                client.bulk(bulkRequest, createListener());
            } else if (continueReceivingData()) {
                request.contentStream().next();
            } else {
                client.bulk(bulkRequest, createListener());
            }
        }

        private void accountParsing(int bytesConsumed) {
            while (bytesConsumed > 0) {
                int length = unParsedChunks.peek().length() - offset;
                if (bytesConsumed > length) {
                    parsedChunks.add(unParsedChunks.removeFirst());
                    offset = 0;
                    bytesConsumed -= length;
                } else {
                    offset = length - bytesConsumed;
                    bytesConsumed = 0;
                }
            }
        }

        private ActionListener<BulkResponse> createListener() {
            return new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkResponse) {
                    responses.add(bulkResponse);
                    parsedChunks.forEach(ReleasableBytesReference::close);
                    parsedChunks.clear();

                    if (isDone) {
                        assert unParsedChunks.isEmpty();
                        listener.onResponse(combineResponses());
                    } else {
                        createNewBulkRequest();
                        request.contentStream().next();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: This is a global error. How do we handle this? Perhaps iterate through the remaining data dropping it on
                    // the floor? Or send an early response and close the channel?
                    listener.onFailure(e);
                }
            };
        }

        private BulkResponse combineResponses() {
            long tookInMillis = 0;
            long ingestTookInMillis = 0;
            int itemResponseCount = 0;
            for (BulkResponse response : responses) {
                tookInMillis += response.getTookInMillis();
                ingestTookInMillis += response.getIngestTookInMillis();
                itemResponseCount += response.getItems().length;
            }
            BulkItemResponse[] bulkItemResponses = new BulkItemResponse[itemResponseCount];
            int i = 0;
            for (BulkResponse response : responses) {
                for (BulkItemResponse itemResponse : response.getItems()) {
                    bulkItemResponses[i++] = itemResponse;
                }
            }

            return new BulkResponse(bulkItemResponses, tookInMillis, ingestTookInMillis);
        }
    }

    private boolean continueReceivingData() {
        return true;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestParser;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.bulk.IncrementalBulkService;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

/**
 * <pre>
 * { "index" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * { "delete" : { "_index" : "test", "_id" : "2" } }
 * { "create" : { "_index" : "test", "_id" : "1" }
 * { "type1" : { "field1" : "value1" } }
 * </pre>
 */
@ServerlessScope(Scope.PUBLIC)
public class RestBulkAction extends BaseRestHandler {

    public static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Specifying types in bulk requests is deprecated.";
    public static final String FAILURE_STORE_STATUS_CAPABILITY = "failure_store_status";

    private final boolean allowExplicitIndex;
    private final IncrementalBulkService bulkHandler;
    private final Set<String> capabilities;

    public RestBulkAction(Settings settings, IncrementalBulkService bulkHandler) {
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
        this.bulkHandler = bulkHandler;
        this.capabilities = DataStream.isFailureStoreFeatureFlagEnabled() ? Set.of(FAILURE_STORE_STATUS_CAPABILITY) : Set.of();
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(POST, "/_bulk"),
            new Route(PUT, "/_bulk"),
            new Route(POST, "/{index}/_bulk"),
            new Route(PUT, "/{index}/_bulk"),
            Route.builder(POST, "/{index}/{type}/_bulk").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build(),
            Route.builder(PUT, "/{index}/{type}/_bulk").deprecated(TYPES_DEPRECATION_MESSAGE, RestApiVersion.V_7).build()
        );
    }

    @Override
    public String getName() {
        return "bulk_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        if (request.isStreamedContent() == false) {
            if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
                request.param("type");
            }
            BulkRequest bulkRequest = new BulkRequest();
            String defaultIndex = request.param("index");
            String defaultRouting = request.param("routing");
            FetchSourceContext defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            String defaultPipeline = request.param("pipeline");
            boolean defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
            String waitForActiveShards = request.param("wait_for_active_shards");
            if (waitForActiveShards != null) {
                bulkRequest.waitForActiveShards(ActiveShardCount.parseString(waitForActiveShards));
            }
            Boolean defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
            boolean defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
            bulkRequest.timeout(request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT));
            bulkRequest.setRefreshPolicy(request.param("refresh"));
            bulkRequest.add(
                request.requiredContent(),
                defaultIndex,
                defaultRouting,
                defaultFetchSourceContext,
                defaultPipeline,
                defaultRequireAlias,
                defaultRequireDataStream,
                defaultListExecutedPipelines,
                allowExplicitIndex,
                request.getXContentType(),
                request.getRestApiVersion()
            );

            return channel -> client.bulk(bulkRequest, new RestRefCountedChunkedToXContentListener<>(channel));
        } else {
            if (request.getRestApiVersion() == RestApiVersion.V_7 && request.hasParam("type")) {
                request.param("type");
            }

            String waitForActiveShards = request.param("wait_for_active_shards");
            TimeValue timeout = request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT);
            String refresh = request.param("refresh");
            return new ChunkHandler(allowExplicitIndex, request, () -> bulkHandler.newBulkRequest(waitForActiveShards, timeout, refresh));
        }
    }

    static class ChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

        private final boolean allowExplicitIndex;
        private final RestRequest request;

        private final Map<String, String> stringDeduplicator = new HashMap<>();
        private final String defaultIndex;
        private final String defaultRouting;
        private final FetchSourceContext defaultFetchSourceContext;
        private final String defaultPipeline;
        private final boolean defaultListExecutedPipelines;
        private final Boolean defaultRequireAlias;
        private final boolean defaultRequireDataStream;
        private final BulkRequestParser parser;
        private final Supplier<IncrementalBulkService.Handler> handlerSupplier;
        private IncrementalBulkService.Handler handler;

        private volatile RestChannel restChannel;
        private boolean shortCircuited;
        private int bytesParsed = 0;
        private final ArrayDeque<ReleasableBytesReference> unParsedChunks = new ArrayDeque<>(4);
        private final ArrayList<DocWriteRequest<?>> items = new ArrayList<>(4);

        ChunkHandler(boolean allowExplicitIndex, RestRequest request, Supplier<IncrementalBulkService.Handler> handlerSupplier) {
            this.allowExplicitIndex = allowExplicitIndex;
            this.request = request;
            this.defaultIndex = request.param("index");
            this.defaultRouting = request.param("routing");
            this.defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            this.defaultPipeline = request.param("pipeline");
            this.defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
            this.defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
            this.defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
            // TODO: Fix type deprecation logging
            this.parser = new BulkRequestParser(false, request.getRestApiVersion());
            this.handlerSupplier = handlerSupplier;
        }

        @Override
        public void accept(RestChannel restChannel) {
            this.restChannel = restChannel;
            this.handler = handlerSupplier.get();
            request.contentStream().next();
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            assert handler != null;
            assert channel == restChannel;
            if (shortCircuited) {
                chunk.close();
                return;
            }

            final BytesReference data;
            int bytesConsumed;
            if (chunk.length() == 0) {
                chunk.close();
                bytesConsumed = 0;
            } else {
                try {
                    unParsedChunks.add(chunk);

                    if (unParsedChunks.size() > 1) {
                        data = CompositeBytesReference.of(unParsedChunks.toArray(new ReleasableBytesReference[0]));
                    } else {
                        data = chunk;
                    }

                    // TODO: Check that the behavior here vs. globalRouting, globalPipeline, globalRequireAlias, globalRequireDatsStream in
                    // BulkRequest#add is fine
                    bytesConsumed = parser.incrementalParse(
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
                        (request, type) -> items.add(request),
                        items::add,
                        items::add,
                        isLast == false,
                        stringDeduplicator
                    );
                    bytesParsed += bytesConsumed;

                } catch (Exception e) {
                    shortCircuit();
                    new RestToXContentListener<>(channel).onFailure(
                        new ElasticsearchParseException("could not parse bulk request body", e)
                    );
                    return;
                }
            }

            final ArrayList<Releasable> releasables = accountParsing(bytesConsumed);
            if (isLast) {
                assert unParsedChunks.isEmpty();
                if (bytesParsed == 0) {
                    shortCircuit();
                    new RestToXContentListener<>(channel).onFailure(new ElasticsearchParseException("request body is required"));
                } else {
                    assert channel != null;
                    ArrayList<DocWriteRequest<?>> toPass = new ArrayList<>(items);
                    items.clear();
                    handler.lastItems(toPass, () -> Releasables.close(releasables), new RestRefCountedChunkedToXContentListener<>(channel));
                }
            } else if (items.isEmpty() == false) {
                ArrayList<DocWriteRequest<?>> toPass = new ArrayList<>(items);
                items.clear();
                handler.addItems(toPass, () -> Releasables.close(releasables), () -> request.contentStream().next());
            } else {
                assert releasables.isEmpty();
                request.contentStream().next();
            }
        }

        @Override
        public void streamClose() {
            assert Transports.assertTransportThread();
            shortCircuit();
        }

        private void shortCircuit() {
            shortCircuited = true;
            Releasables.close(handler);
            Releasables.close(unParsedChunks);
            unParsedChunks.clear();
        }

        private ArrayList<Releasable> accountParsing(int bytesConsumed) {
            ArrayList<Releasable> releasables = new ArrayList<>(unParsedChunks.size());
            while (bytesConsumed > 0) {
                ReleasableBytesReference reference = unParsedChunks.removeFirst();
                releasables.add(reference);
                if (bytesConsumed >= reference.length()) {
                    bytesConsumed -= reference.length();
                } else {
                    unParsedChunks.addFirst(reference.retainedSlice(bytesConsumed, reference.length() - bytesConsumed));
                    bytesConsumed = 0;
                }
            }
            return releasables;
        }
    }

    @Override
    public boolean supportsBulkContent() {
        return true;
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return true;
    }

    @Override
    public Set<String> supportedCapabilities() {
        return capabilities;
    }
}

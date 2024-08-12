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
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestRefCountedChunkedToXContentListener;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IncrementalBulkNetwork {

    private final boolean allowExplicitIndex;
    private final IncrementalBulkApplication incrementalBulkApplication;

    public IncrementalBulkNetwork(Settings settings, IncrementalBulkApplication incrementalBulkApplication) {
        this.allowExplicitIndex = BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX.get(settings);
        this.incrementalBulkApplication = incrementalBulkApplication;
    }

    public BaseRestHandler.RequestBodyChunkConsumer newBulkRequest(RestRequest request, ActionListener<BulkResponse> listener) {
        return new ChunkHandler(request, listener);
    }

    private class ChunkHandler implements BaseRestHandler.RequestBodyChunkConsumer {

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
        private final IncrementalBulkApplication.Handler handler;

        private volatile RestChannel restChannel;
        private final ArrayDeque<ReleasableBytesReference> unParsedChunks = new ArrayDeque<>(4);
        private final ArrayList<DocWriteRequest<?>> items = new ArrayList<>(4);

        private ChunkHandler(RestRequest request, ActionListener<BulkResponse> listener) {
            this.request = request;
            this.defaultIndex = request.param("index");
            this.defaultRouting = request.param("routing");
            this.defaultFetchSourceContext = FetchSourceContext.parseFromRestRequest(request);
            this.defaultPipeline = request.param("pipeline");
            this.defaultListExecutedPipelines = request.paramAsBoolean("list_executed_pipelines", false);
            this.defaultRequireAlias = request.paramAsBoolean(DocWriteRequest.REQUIRE_ALIAS, false);
            this.defaultRequireDataStream = request.paramAsBoolean(DocWriteRequest.REQUIRE_DATA_STREAM, false);
            this.parser = new BulkRequestParser(true, request.getRestApiVersion());
            handler = incrementalBulkApplication.newBulkRequest(
                request.param("wait_for_active_shards"),
                request.paramAsTime("timeout", BulkShardRequest.DEFAULT_TIMEOUT),
                request.param("refresh")
            );
        }

        @Override
        public void accept(RestChannel restChannel) throws Exception {
            this.restChannel = restChannel;
        }

        @Override
        public void handleChunk(RestChannel channel, ReleasableBytesReference chunk, boolean isLast) {
            assert channel == restChannel;

            final ReleasableBytesReference data;
            try {
                // TODO: Check that the behavior here vs. globalRouting, globalPipeline, globalRequireAlias, globalRequireDatsStream in
                // BulkRequest#add is fine

                unParsedChunks.add(chunk);

                if (unParsedChunks.size() > 1) {
                    ReleasableBytesReference[] bytesReferences = unParsedChunks.toArray(new ReleasableBytesReference[0]);
                    data = new ReleasableBytesReference(
                        CompositeBytesReference.of(bytesReferences),
                        () -> Releasables.close(bytesReferences)
                    );
                } else {
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
                    (request, type) -> items.add(request),
                    items::add,
                    items::add,
                    stringDeduplicator
                );

                accountParsing(bytesConsumed);

            } catch (IOException e) {
                // TODO: Exception Handling
                throw new UncheckedIOException(e);
            }

            if (isLast) {
                assert unParsedChunks.isEmpty();
                assert channel != null;
                handler.lastItems(items, data, new RestRefCountedChunkedToXContentListener<>(channel));
            } else {
                handler.addItems(items, data, () -> request.contentStream().next());
            }
        }

        private void accountParsing(int bytesConsumed) {
            while (bytesConsumed > 0) {
                ReleasableBytesReference reference = unParsedChunks.removeFirst();
                if (bytesConsumed >= reference.length()) {
                    bytesConsumed -= reference.length();
                } else {
                    unParsedChunks.addFirst(reference.retainedSlice(bytesConsumed, reference.length() - bytesConsumed));
                    bytesConsumed = 0;
                }
            }
        }
    }
}

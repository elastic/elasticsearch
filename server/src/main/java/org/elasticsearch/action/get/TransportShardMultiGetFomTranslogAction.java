/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

// TODO(ES-5727): add a retry mechanism to TransportShardMultiGetFromTranslogAction
public class TransportShardMultiGetFomTranslogAction extends HandledTransportAction<
    TransportShardMultiGetFomTranslogAction.Request,
    TransportShardMultiGetFomTranslogAction.Response> {

    public static final String NAME = "internal:data/read/mget_from_translog[shard]";
    public static final Logger logger = LogManager.getLogger(TransportShardMultiGetFomTranslogAction.class);

    private final IndicesService indicesService;

    protected TransportShardMultiGetFomTranslogAction(
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters
    ) {
        super(NAME, transportService, actionFilters, Request::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        var multiGetShardRequest = request.getMultiGetShardRequest();
        var shardId = request.getShardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        assert indexShard.routingEntry().isPromotableToPrimary() : "not an indexing shard" + indexShard.routingEntry();
        assert multiGetShardRequest.realtime();
        ActionListener.completeWith(listener, () -> {
            var multiGetShardResponse = new MultiGetShardResponse();
            var someItemsNotFoundInTranslog = false;
            for (int i = 0; i < multiGetShardRequest.locations.size(); i++) {
                var item = multiGetShardRequest.items.get(i);
                try {
                    var result = indexShard.getService()
                        .getFromTranslog(
                            item.id(),
                            item.storedFields(),
                            multiGetShardRequest.realtime(),
                            item.version(),
                            item.versionType(),
                            item.fetchSourceContext(),
                            multiGetShardRequest.isForceSyntheticSource()
                        );
                    GetResponse getResponse = null;
                    if (result == null) {
                        someItemsNotFoundInTranslog = true;
                    } else {
                        getResponse = new GetResponse(result);
                    }
                    multiGetShardResponse.add(multiGetShardRequest.locations.get(i), getResponse);
                } catch (RuntimeException | IOException e) {
                    if (TransportActions.isShardNotAvailableException(e)) {
                        throw e;
                    }
                    logger.debug("failed to execute multi_get_from_translog for {}[id={}]: {}", shardId, item.id(), e);
                    multiGetShardResponse.add(
                        multiGetShardRequest.locations.get(i),
                        new MultiGetResponse.Failure(multiGetShardRequest.index(), item.id(), e)
                    );
                }
            }
            long segmentGeneration = -1;
            if (someItemsNotFoundInTranslog) {
                Engine engine = indexShard.getEngineOrNull();
                if (engine == null) {
                    throw new AlreadyClosedException("engine closed");
                }
                segmentGeneration = ((InternalEngine) engine).getLastUnsafeSegmentGenerationForGets();
            }
            return new Response(multiGetShardResponse, segmentGeneration);
        });
    }

    public static class Request extends ActionRequest {

        private final MultiGetShardRequest multiGetShardRequest;
        private final ShardId shardId;

        public Request(MultiGetShardRequest multiGetShardRequest, ShardId shardId) {
            this.multiGetShardRequest = multiGetShardRequest;
            this.shardId = shardId;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            multiGetShardRequest = new MultiGetShardRequest(in);
            shardId = new ShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            multiGetShardRequest.writeTo(out);
            shardId.writeTo(out);
        }

        public MultiGetShardRequest getMultiGetShardRequest() {
            return multiGetShardRequest;
        }

        public ShardId getShardId() {
            return shardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "ShardMultiGetFomTranslogRequest{" + "multiGetShardRequest=" + multiGetShardRequest + ", shardId=" + shardId + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Request == false) return false;
            Request other = (Request) o;
            return Objects.equals(shardId, other.shardId) && Objects.equals(multiGetShardRequest, other.multiGetShardRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardId, multiGetShardRequest);
        }
    }

    public static class Response extends ActionResponse {

        private final MultiGetShardResponse multiGetShardResponse;
        private final long segmentGeneration;

        public Response(MultiGetShardResponse response, long segmentGeneration) {
            this.segmentGeneration = segmentGeneration;
            this.multiGetShardResponse = response;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            segmentGeneration = in.readZLong();
            multiGetShardResponse = new MultiGetShardResponse(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(segmentGeneration);
            multiGetShardResponse.writeTo(out);
        }

        public long segmentGeneration() {
            return segmentGeneration;
        }

        public MultiGetShardResponse multiGetShardResponse() {
            return multiGetShardResponse;
        }

        @Override
        public String toString() {
            return "ShardMultiGetFomTranslogResponse{"
                + "multiGetShardResponse="
                + multiGetShardResponse
                + ", segmentGeneration="
                + segmentGeneration
                + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response other = (Response) o;
            return segmentGeneration == other.segmentGeneration && Objects.equals(multiGetShardResponse, other.multiGetShardResponse);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentGeneration, multiGetShardResponse);
        }
    }
}

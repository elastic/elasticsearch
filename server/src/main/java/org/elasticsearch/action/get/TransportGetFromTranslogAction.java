/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.get.GetResult;
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

// TODO(ES-5727): add a retry mechanism to TransportGetFromTranslogAction
public class TransportGetFromTranslogAction extends HandledTransportAction<
    TransportGetFromTranslogAction.Request,
    TransportGetFromTranslogAction.Response> {

    public static final String NAME = "internal:data/read/get_from_translog";
    public static final Logger logger = LogManager.getLogger(TransportGetFromTranslogAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetFromTranslogAction(TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters, Request::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final GetRequest getRequest = request.getRequest();
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        assert indexShard.routingEntry().isPromotableToPrimary() : "not an indexing shard" + indexShard.routingEntry();
        assert getRequest.realtime();
        ActionListener.completeWith(listener, () -> {
            var result = indexShard.getService()
                .getFromTranslog(
                    getRequest.id(),
                    getRequest.storedFields(),
                    getRequest.realtime(),
                    getRequest.version(),
                    getRequest.versionType(),
                    getRequest.fetchSourceContext(),
                    getRequest.isForceSyntheticSource()
                );
            long segmentGeneration = -1;
            if (result == null) {
                Engine engine = indexShard.getEngineOrNull();
                if (engine == null) {
                    throw new AlreadyClosedException("engine closed");
                }
                segmentGeneration = ((InternalEngine) engine).getLastUnsafeSegmentGenerationForGets();
            }
            return new Response(result, segmentGeneration);
        });
    }

    public static class Request extends ActionRequest {

        private final GetRequest getRequest;
        private final ShardId shardId;

        public Request(GetRequest getRequest, ShardId shardId) {
            this.getRequest = Objects.requireNonNull(getRequest);
            this.shardId = Objects.requireNonNull(shardId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            getRequest = new GetRequest(in);
            shardId = new ShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            getRequest.writeTo(out);
            shardId.writeTo(out);
        }

        public GetRequest getRequest() {
            return getRequest;
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public String toString() {
            return "GetFromTranslogRequest{" + "getRequest=" + getRequest + ", shardId=" + shardId + "}";
        }
    }

    public static class Response extends ActionResponse {
        @Nullable
        private final GetResult getResult;
        private final long segmentGeneration;

        public Response(GetResult getResult, long segmentGeneration) {
            this.getResult = getResult;
            this.segmentGeneration = segmentGeneration;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            segmentGeneration = in.readZLong();
            getResult = in.readOptionalWriteable(GetResult::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(segmentGeneration);
            out.writeOptionalWriteable(getResult);
        }

        @Nullable
        public GetResult getResult() {
            return getResult;
        }

        /**
         * The segment generation that the search shard should wait for before handling the real-time GET request locally.
         * -1 if the result is not null (i.e., the result is served from the indexing shard), or there hasn't simply been
         * any switches from unsafe to safe map in the LiveVersionMap (see {@link InternalEngine#getVersionFromMap(BytesRef)}).
         */
        public long segmentGeneration() {
            return segmentGeneration;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response other = (Response) o;
            return segmentGeneration == other.segmentGeneration && Objects.equals(getResult, other.getResult);
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentGeneration, getResult);
        }

        @Override
        public String toString() {
            return "Response{" + "getResult=" + getResult + ", segmentGeneration=" + segmentGeneration + "}";
        }
    }
}

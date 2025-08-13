/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.get;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class TransportGetFromTranslogAction extends HandledTransportAction<
    TransportGetFromTranslogAction.Request,
    TransportGetFromTranslogAction.Response> {

    public static final String NAME = "indices:data/read/get_from_translog";
    public static final Logger logger = LogManager.getLogger(TransportGetFromTranslogAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetFromTranslogAction(TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters, Request::new, transportService.getThreadPool().executor(ThreadPool.Names.GET));
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
            // Allows to keep the same engine instance for getFromTranslog and getLastUnsafeSegmentGenerationForGets
            return indexShard.withEngineException(engine -> {
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
                    segmentGeneration = engine.getLastUnsafeSegmentGenerationForGets();
                }
                return new Response(result, indexShard.getOperationPrimaryTerm(), segmentGeneration);
            });
        });
    }

    public static class Request extends LegacyActionRequest implements IndicesRequest {

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

        @Override
        public String[] indices() {
            return getRequest.indices();
        }

        @Override
        public IndicesOptions indicesOptions() {
            return getRequest.indicesOptions();
        }
    }

    public static class Response extends ActionResponse {
        @Nullable
        private final GetResult getResult;
        private final long primaryTerm;
        private final long segmentGeneration;

        public Response(GetResult getResult, long primaryTerm, long segmentGeneration) {
            this.getResult = getResult;
            this.segmentGeneration = segmentGeneration;
            this.primaryTerm = primaryTerm;
        }

        public Response(StreamInput in) throws IOException {
            segmentGeneration = in.readZLong();
            getResult = in.readOptionalWriteable(GetResult::new);
            primaryTerm = in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0) ? in.readVLong() : Engine.UNKNOWN_PRIMARY_TERM;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(segmentGeneration);
            out.writeOptionalWriteable(getResult);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeVLong(primaryTerm);
            }
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

        public long primaryTerm() {
            return primaryTerm;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response response = (Response) o;
            return segmentGeneration == response.segmentGeneration
                && Objects.equals(getResult, response.getResult)
                && primaryTerm == response.primaryTerm;
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentGeneration, getResult, primaryTerm);
        }

        @Override
        public String toString() {
            return Strings.format(
                "Response{getResult=%s, primaryTerm=%d, segmentGeneration=%d}",
                getResult,
                primaryTerm,
                segmentGeneration
            );
        }
    }
}

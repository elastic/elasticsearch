/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class RetentionLeaseActions {

    static abstract class TransportRetentionLeaseAction extends TransportSingleShardAction<Request, Response> {

        private Logger logger = LogManager.getLogger(getClass());

        private final IndicesService indicesService;

        @Inject
        public TransportRetentionLeaseAction(
                final String name,
                final ThreadPool threadPool,
                final ClusterService clusterService,
                final TransportService transportService,
                final ActionFilters actionFilters,
                final IndexNameExpressionResolver indexNameExpressionResolver,
                final IndicesService indicesService) {
            super(name, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, Request::new, ThreadPool.Names.MANAGEMENT);
            this.indicesService = Objects.requireNonNull(indicesService);
        }

        @Override
        protected ShardsIterator shards(ClusterState state, InternalRequest request) {
            return state
                    .routingTable()
                    .shardRoutingTable(request.concreteIndex(), request.request().getShardId().id())
                    .primaryShardIt();
        }

        @Override
        protected Response shardOperation(final Request request, final ShardId shardId) {
            final IndexService indexService = indicesService.indexServiceSafe(request.getShardId().getIndex());
            final IndexShard indexShard = indexService.getShard(request.getShardId().id());

            final CompletableFuture<Releasable> permit = new CompletableFuture<>();
            final ActionListener<Releasable> onAcquired = new ActionListener<Releasable>() {

                @Override
                public void onResponse(Releasable releasable) {
                    if (permit.complete(releasable) == false) {
                        releasable.close();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    permit.completeExceptionally(e);
                }

            };

            indexShard.acquirePrimaryOperationPermit(onAcquired, ThreadPool.Names.SAME, request);

            // block until we have the permit
            try (Releasable ignore = FutureUtils.get(permit)) {
                doRetentionLeaseAction(indexShard, request);
            } finally {
                // just in case we got an exception (likely interrupted) while waiting for the get
                permit.whenComplete((r, e) -> {
                    if (r != null) {
                        r.close();
                    }
                    if (e != null) {
                        logger.trace("suppressing exception on completion (it was already bubbled up or the operation was aborted)", e);
                    }
                });
            }

            return new Response();
        }

        abstract void doRetentionLeaseAction(IndexShard indexShard, Request request);

        @Override
        protected Response newResponse() {
            return new Response();
        }

        @Override
        protected boolean resolveIndex(final Request request) {
            return false;
        }

    }

    public static class Add extends Action<Response> {

        public static final Add INSTANCE = new Add();
        public static final String NAME = "indices:admin/seq_no/add_retention_lease";

        private Add() {
            super(NAME);
        }

        public static class TransportAction extends TransportRetentionLeaseAction {

            @Inject
            public TransportAction(
                    final ThreadPool threadPool,
                    final ClusterService clusterService,
                    final TransportService transportService,
                    final ActionFilters actionFilters,
                    final IndexNameExpressionResolver indexNameExpressionResolver,
                    final IndicesService indicesService) {
                super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, indicesService);
            }

            @Override
            void doRetentionLeaseAction(final IndexShard indexShard, final Request request) {
                indexShard.addRetentionLease(request.getId(), request.getRetainingSequenceNumber(), request.getSource(), ActionListener.wrap(() -> {}));
            }
        }

        @Override
        public Response newResponse() {
            return new Response();
        }

    }

    public static class Renew extends Action<Response> {

        public static final Renew INSTANCE = new Renew();
        public static final String NAME = "indices:admin/seq_no/renew_retention_lease";

        private Renew() {
            super(NAME);
        }

        public static class TransportAction extends TransportRetentionLeaseAction {

            @Inject
            public TransportAction(
                    final ThreadPool threadPool,
                    final ClusterService clusterService,
                    final TransportService transportService,
                    final ActionFilters actionFilters,
                    final IndexNameExpressionResolver indexNameExpressionResolver,
                    final IndicesService indicesService) {
                super(NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver, indicesService);
            }


            @Override
            void doRetentionLeaseAction(final IndexShard indexShard, final Request request) {
                indexShard.renewRetentionLease(request.getId(), request.getRetainingSequenceNumber(), request.getSource());
            }
        }

        @Override
        public Response newResponse() {
            return new Response();
        }

    }

    public static class Request extends SingleShardRequest<Request> {

        public static long RETAIN_ALL = -1;

        private ShardId shardId;

        public ShardId getShardId() {
            return shardId;
        }

        private String id;

        public String getId() {
            return id;
        }

        private long retainingSequenceNumber;

        public long getRetainingSequenceNumber() {
            return retainingSequenceNumber;
        }

        private String source;

        public String getSource() {
            return source;
        }

        public Request() {
        }

        public Request(final ShardId shardId, final String id, final long retainingSequenceNumber, final String source) {
            super(Objects.requireNonNull(shardId).getIndexName());
            this.shardId = shardId;
            this.id = Objects.requireNonNull(id);
            if (retainingSequenceNumber < 0 && retainingSequenceNumber != RETAIN_ALL) {
                throw new IllegalArgumentException(
                        "retention lease retaining sequence number [" + retainingSequenceNumber + "] out of range");
            }
            this.retainingSequenceNumber = retainingSequenceNumber;
            this.source = Objects.requireNonNull(source);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            id = in.readString();
            retainingSequenceNumber = in.readZLong();
            source = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeString(id);
            out.writeZLong(retainingSequenceNumber);
            out.writeString(source);
        }

    }

    public static class Response extends ActionResponse {

    }

}
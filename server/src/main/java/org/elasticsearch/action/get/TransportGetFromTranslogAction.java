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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.IdFieldMapper;
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

public class TransportGetFromTranslogAction extends HandledTransportAction<GetRequest, TransportGetFromTranslogAction.Response> {

    public static final String NAME = "internal:data/read/get_from_translog";
    public static final Logger logger = LogManager.getLogger(TransportGetFromTranslogAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetFromTranslogAction(TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters, GetRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, GetRequest request, ActionListener<Response> listener) {
        final ShardId shardId = request.getInternalShardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        assert indexShard.routingEntry().role().equals(ShardRouting.Role.INDEX_ONLY);
        assert request.realtime();
        Engine engine = indexShard.getEngineOrNull();
        if (engine == null) {
            listener.onFailure(new AlreadyClosedException("engine closed"));
            return;
        }
        assert engine instanceof InternalEngine;
        var internalEngine = (InternalEngine) engine;
        var get = new Engine.Get(request.realtime(), request.realtime(), request.id()).version(request.version())
            .versionType(request.versionType());
        assert Objects.equals(get.uid().field(), IdFieldMapper.NAME) : get.uid().field();
        ActionListener.completeWith(listener, () -> {
            long segmentGeneration = internalEngine.getLastCommittedSegmentInfos().getGeneration();
            if (internalEngine.isInVersionMap(get.uid().bytes()) == false) {
                return new Response(null, segmentGeneration);
            }
            var result = indexShard.getService()
                .get(
                    request.id(),
                    request.storedFields(),
                    request.realtime(),
                    request.version(),
                    request.versionType(),
                    request.fetchSourceContext(),
                    request.isForceSyntheticSource()
                );
            return new Response(result, segmentGeneration);
        });
    }

    public static class Response extends ActionResponse {
        private GetResult getResult;
        private final long segmentGeneration;

        public Response(GetResult getResult, long segmentGeneration) {
            this.getResult = getResult;
            this.segmentGeneration = segmentGeneration;
        }

        Response(StreamInput in) throws IOException {
            super(in);
            segmentGeneration = in.readVLong();
            if (in.readBoolean()) {
                getResult = new GetResult(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(segmentGeneration);
            if (getResult == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                getResult.writeTo(out);
            }
        }

        public GetResult getResult() {
            return getResult;
        }

        public long segmentGeneration() {
            return segmentGeneration;
        }
    }
}

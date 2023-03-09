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
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
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

public class TransportGetFromTranslogAction extends HandledTransportAction<MultiGetShardRequest, TransportGetFromTranslogAction.Response> {

    public static final String NAME = "internal:data/read/get_from_translog";
    public static final Logger logger = LogManager.getLogger(TransportGetFromTranslogAction.class);

    private final IndicesService indicesService;

    @Inject
    public TransportGetFromTranslogAction(TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(NAME, transportService, actionFilters, MultiGetShardRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(Task task, MultiGetShardRequest request, ActionListener<Response> listener) {
        final ShardId shardId = request.getInternalShardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        assert indexShard.routingEntry().isPromotableToPrimary();
        Engine engine = indexShard.getEngineOrNull();
        if (engine == null) {
            listener.onFailure(new AlreadyClosedException("engine closed"));
            return;
        }
        assert request.realtime();
        ActionListener.completeWith(listener, () -> {
            Response response = new Response();
            boolean includeSegmentGeneration = false;
            for (int i = 0; i < request.locations.size(); i++) {
                MultiGetRequest.Item item = request.items.get(i);
                var get = new Engine.Get(request.realtime(), request.realtime(), item.id());
                assert Objects.equals(get.uid().field(), IdFieldMapper.NAME) : get.uid().field();
                if (engine.isInVersionMap(get.uid().bytes())) {
                    try {
                        var result = indexShard.getService()
                            .get(
                                item.id(),
                                item.storedFields(),
                                request.realtime(),
                                item.version(),
                                item.versionType(),
                                item.fetchSourceContext(),
                                request.isForceSyntheticSource()
                            );
                        response.addResult(request.locations.get(i), new GetResponse(result));
                    } catch (RuntimeException | IOException e) {
                        if (TransportActions.isShardNotAvailableException(e)) {
                            throw e;
                        }
                        logger.debug("failed to execute get_from_translog for {}[id={}]: {}", shardId, item.id(), e);
                        response.addFailure(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
                    }
                } else {
                    includeSegmentGeneration = true;
                    response.addResult(request.locations.get(i), null);
                }
            }
            if (includeSegmentGeneration) {
                response.segmentGeneration(engine.commitStats().getGeneration());
            }
            return response;
        });
    }

    public static class Response extends ActionResponse {
        private final MultiGetShardResponse multiGetShardResponse;
        private long segmentGeneration;

        public Response() {
            this.segmentGeneration = -1;
            this.multiGetShardResponse = new MultiGetShardResponse();
        }

        // for testing
        Response(MultiGetShardResponse response, long segmentGeneration) {
            this.segmentGeneration = segmentGeneration;
            this.multiGetShardResponse = response;
        }

        public void addResult(int location, GetResponse result) {
            multiGetShardResponse.locations.add(location);
            multiGetShardResponse.responses.add(result);
            multiGetShardResponse.failures.add(null);
        }

        public void addFailure(int location, MultiGetResponse.Failure failure) {
            multiGetShardResponse.locations.add(location);
            multiGetShardResponse.responses.add(null);
            multiGetShardResponse.failures.add(failure);
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

        public void segmentGeneration(long segmentGeneration) {
            this.segmentGeneration = segmentGeneration;
        }

        public long segmentGeneration() {
            return segmentGeneration;
        }

        public MultiGetShardResponse multiGetShardResponse() {
            return multiGetShardResponse;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Response == false) return false;
            Response other = (Response) o;
            return Objects.equals(multiGetShardResponse, other.multiGetShardResponse) && segmentGeneration == other.segmentGeneration;
        }

        @Override
        public int hashCode() {
            return Objects.hash(segmentGeneration, multiGetShardResponse);
        }
    }
}

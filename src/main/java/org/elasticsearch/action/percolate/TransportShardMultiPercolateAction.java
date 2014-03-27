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

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.SingleShardOperationRequest;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class TransportShardMultiPercolateAction extends TransportShardSingleOperationAction<TransportShardMultiPercolateAction.Request, TransportShardMultiPercolateAction.Response> {

    private final PercolatorService percolatorService;

    @Inject
    public TransportShardMultiPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, PercolatorService percolatorService) {
        super(settings, threadPool, clusterService, transportService);
        this.percolatorService = percolatorService;
    }

    @Override
    protected String transportAction() {
        return "mpercolate/shard";
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected Request newRequest() {
        return new Request();
    }

    @Override
    protected Response newResponse() {
        return new Response();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, Request request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, Request request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, Request request) throws ElasticsearchException {
        return clusterService.operationRouting().getShards(
                clusterService.state(), request.index(), request.shardId(), request.preference
        );
    }

    @Override
    protected Response shardOperation(Request request, int shardId) throws ElasticsearchException {
        // TODO: Look into combining the shard req's docs into one in memory index.
        Response response = new Response();
        response.items = new ArrayList<>(request.items.size());
        for (Request.Item item : request.items) {
            Response.Item responseItem;
            int slot = item.slot;
            try {
                responseItem = new Response.Item(slot, percolatorService.percolate(item.request));
            } catch (Throwable t) {
                if (TransportActions.isShardNotAvailableException(t)) {
                    throw (ElasticsearchException) t;
                } else {
                    logger.debug("[{}][{}] failed to multi percolate", t, request.index(), request.shardId());
                    responseItem = new Response.Item(slot, new StringText(ExceptionsHelper.detailedMessage(t)));
                }
            }
            response.items.add(responseItem);
        }
        return response;
    }


    public static class Request extends SingleShardOperationRequest {

        private int shardId;
        private String preference;
        private List<Item> items;

        public Request() {
        }

        public Request(String concreteIndex, int shardId, String preference) {
            this.index = concreteIndex;
            this.shardId = shardId;
            this.preference = preference;
            this.items = new ArrayList<>();
        }

        public int shardId() {
            return shardId;
        }

        public void add(Item item) {
            items.add(item);
        }

        public List<Item> items() {
            return items;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = in.readVInt();
            preference = in.readOptionalString();
            int size = in.readVInt();
            items = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int slot = in.readVInt();
                PercolateShardRequest shardRequest = new PercolateShardRequest(index(), shardId);
                shardRequest.documentType(in.readString());
                shardRequest.source(in.readBytesReference());
                shardRequest.docSource(in.readBytesReference());
                shardRequest.onlyCount(in.readBoolean());
                Item item = new Item(slot, shardRequest);
                items.add(item);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(shardId);
            out.writeOptionalString(preference);
            out.writeVInt(items.size());
            for (Item item : items) {
                out.writeVInt(item.slot);
                out.writeString(item.request.documentType());
                out.writeBytesReference(item.request.source());
                out.writeBytesReference(item.request.docSource());
                out.writeBoolean(item.request.onlyCount());
            }
        }

        public static class Item {

            private final int slot;
            private final PercolateShardRequest request;

            public Item(int slot, PercolateShardRequest request) {
                this.slot = slot;
                this.request = request;
            }

            public int slot() {
                return slot;
            }

            public PercolateShardRequest request() {
                return request;
            }

        }

    }

    public static class Response extends ActionResponse {

        private List<Item> items;

        public List<Item> items() {
            return items;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(items.size());
            for (Item item : items) {
                out.writeVInt(item.slot);
                if (item.response != null) {
                    out.writeBoolean(true);
                    item.response.writeTo(out);
                } else {
                    out.writeBoolean(false);
                    out.writeText(item.error);
                }
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            int size = in.readVInt();
            items = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int slot = in.readVInt();
                if (in.readBoolean()) {
                    PercolateShardResponse shardResponse = new PercolateShardResponse();
                    shardResponse.readFrom(in);
                    items.add(new Item(slot, shardResponse));
                } else {
                    items.add(new Item(slot, in.readText()));
                }
            }
        }

        public static class Item {

            private final int slot;
            private final PercolateShardResponse response;
            private final Text error;

            public Item(Integer slot, PercolateShardResponse response) {
                this.slot = slot;
                this.response = response;
                this.error = null;
            }

            public Item(Integer slot, Text error) {
                this.slot = slot;
                this.error = error;
                this.response = null;
            }

            public int slot() {
                return slot;
            }

            public PercolateShardResponse response() {
                return response;
            }

            public Text error() {
                return error;
            }

            public boolean failed() {
                return error != null;
            }
        }

    }

}

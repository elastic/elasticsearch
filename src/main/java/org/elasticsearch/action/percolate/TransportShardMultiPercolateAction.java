/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchException;
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
    protected ShardIterator shards(ClusterState state, Request request) throws ElasticSearchException {
        return clusterService.operationRouting().getShards(
                clusterService.state(), request.index(), request.shardId(), request.preference
        );
    }

    @Override
    protected Response shardOperation(Request request, int shardId) throws ElasticSearchException {
        // TODO: Look into combining the shard req's docs into one in memory index.
        Response response = new Response();
        response.items = new ArrayList<Response.Item>(request.items.size());
        for (Request.Item item : request.items) {
            Response.Item responseItem = new Response.Item();
            responseItem.slot = item.slot;
            try {
                responseItem.response = percolatorService.percolate(item.request);
            } catch (Throwable e) {
                logger.debug("[{}][{}] failed to multi percolate", e, request.index(), request.shardId());
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw new ElasticSearchException("", e);
                } else {
                    responseItem.error = new StringText(ExceptionsHelper.detailedMessage(e));
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
            this.items = new ArrayList<Item>();
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
            items = new ArrayList<Item>(size);
            for (int i = 0; i < size; i++) {
                Item item = new Item();
                item.slot = in.readVInt();
                item.request = new PercolateShardRequest(index(), shardId);
                item.request.documentType(in.readString());
                item.request.source(in.readBytesReference());
                item.request.docSource(in.readBytesReference());
                item.request.onlyCount(in.readBoolean());
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

            private int slot;
            private PercolateShardRequest request;

            Item() {
            }

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
            items = new ArrayList<Item>(size);
            for (int i = 0; i < size; i++) {
                Item item = new Item();
                item.slot = in.readVInt();
                if (in.readBoolean()) {
                    item.response = new PercolateShardResponse();
                    item.response.readFrom(in);
                } else {
                    item.error = in.readText();
                }
                items.add(item);
            }
        }

        public static class Item {

            private int slot;
            private PercolateShardResponse response;
            private Text error;

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

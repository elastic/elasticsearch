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

package org.elasticsearch.cluster.action.index;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction extends TransportMasterNodeOperationAction<MappingUpdatedAction.MappingUpdatedRequest, MappingUpdatedAction.MappingUpdatedResponse> {

    private final AtomicLong mappingUpdateOrderGen = new AtomicLong();
    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public MappingUpdatedAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                MetaDataMappingService metaDataMappingService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
    }

    public long generateNextMappingUpdateOrder() {
        return mappingUpdateOrderGen.incrementAndGet();
    }

    @Override
    protected String transportAction() {
        return "cluster/mappingUpdated";
    }

    @Override
    protected String executor() {
        // we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected MappingUpdatedRequest newRequest() {
        return new MappingUpdatedRequest();
    }

    @Override
    protected MappingUpdatedResponse newResponse() {
        return new MappingUpdatedResponse();
    }

    @Override
    protected void masterOperation(final MappingUpdatedRequest request, final ClusterState state, final ActionListener<MappingUpdatedResponse> listener) throws ElasticSearchException {
        metaDataMappingService.updateMapping(request.index(), request.indexUUID(), request.type(), request.mappingSource(), request.order, request.nodeId, new ClusterStateUpdateListener() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new MappingUpdatedResponse());
            }

            @Override
            public void onFailure(Throwable t) {
                logger.warn("[{}] update-mapping [{}] failed to dynamically update the mapping in cluster_state from shard", t, request.index(), request.type());
                listener.onFailure(t);
            }
        });
    }

    public static class MappingUpdatedResponse extends ActionResponse {
        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }

    public static class MappingUpdatedRequest extends MasterNodeOperationRequest<MappingUpdatedRequest> {

        private String index;
        private String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;
        private String type;
        private CompressedString mappingSource;
        private long order = -1; // -1 means not set...
        private String nodeId = null; // null means not set

        MappingUpdatedRequest() {
        }

        public MappingUpdatedRequest(String index, String indexUUID, String type, CompressedString mappingSource, long order, String nodeId) {
            this.index = index;
            this.indexUUID = indexUUID;
            this.type = type;
            this.mappingSource = mappingSource;
            this.order = order;
            this.nodeId = nodeId;
        }

        public String index() {
            return index;
        }

        public String indexUUID() {
            return indexUUID;
        }

        public String type() {
            return type;
        }

        public CompressedString mappingSource() {
            return mappingSource;
        }

        /**
         * Returns -1 if not set...
         */
        public long order() {
            return this.order;
        }

        /**
         * Returns null for not set.
         */
        public String nodeId() {
            return this.nodeId;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readString();
            type = in.readString();
            mappingSource = CompressedString.readCompressedString(in);
            if (in.getVersion().onOrAfter(Version.V_0_90_6)) {
                indexUUID = in.readString();
            }
            if (in.getVersion().after(Version.V_0_90_7)) {
                order = in.readLong();
                nodeId = in.readOptionalString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeString(type);
            mappingSource.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_0_90_6)) {
                out.writeString(indexUUID);
            }
            if (out.getVersion().after(Version.V_0_90_7)) {
                out.writeLong(order);
                out.writeOptionalString(nodeId);
            }
        }

        @Override
        public String toString() {
            return "index [" + index + "], indexUUID [" + indexUUID + "], type [" + type + "] and source [" + mappingSource + "]";
        }
    }
}
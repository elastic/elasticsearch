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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Called by shards in the cluster when their mapping was dynamically updated and it needs to be updated
 * in the cluster state meta data (and broadcast to all members).
 */
public class MappingUpdatedAction extends TransportMasterNodeOperationAction<MappingUpdatedAction.MappingUpdatedRequest, MappingUpdatedAction.MappingUpdatedResponse> {

    private final MetaDataMappingService metaDataMappingService;

    @Inject
    public MappingUpdatedAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                MetaDataMappingService metaDataMappingService) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
    }

    @Override
    protected String transportAction() {
        return "cluster/mappingUpdated";
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
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
    protected MappingUpdatedResponse masterOperation(MappingUpdatedRequest request, ClusterState state) throws ElasticSearchException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<Throwable>();
        try {
            metaDataMappingService.updateMapping(request.index(), request.type(), request.mappingSource(), new MetaDataMappingService.Listener() {
                @Override
                public void onResponse(MetaDataMappingService.Response response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    failure.set(t);
                    latch.countDown();
                }
            });
        } catch (Exception e) {
            failure.set(e);
        }
        if (failure.get() != null) {
            if (failure.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failure.get();
            } else {
                throw new ElasticSearchException("failed to update mapping", failure.get());
            }
        }
        return new MappingUpdatedResponse();
    }

    public static class MappingUpdatedResponse implements ActionResponse {
        @Override
        public void readFrom(StreamInput in) throws IOException {
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }
    }

    public static class MappingUpdatedRequest extends MasterNodeOperationRequest {

        private String index;

        private String type;

        private CompressedString mappingSource;

        MappingUpdatedRequest() {
        }

        public MappingUpdatedRequest(String index, String type, CompressedString mappingSource) {
            this.index = index;
            this.type = type;
            this.mappingSource = mappingSource;
        }

        public String index() {
            return index;
        }

        public String type() {
            return type;
        }

        public CompressedString mappingSource() {
            return mappingSource;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            index = in.readUTF();
            type = in.readUTF();
            mappingSource = CompressedString.readCompressedString(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeUTF(index);
            out.writeUTF(type);
            mappingSource.writeTo(out);
        }
    }
}
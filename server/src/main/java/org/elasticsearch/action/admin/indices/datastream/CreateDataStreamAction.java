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
package org.elasticsearch.action.admin.indices.datastream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class CreateDataStreamAction extends ActionType<AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(CreateDataStreamAction.class);

    public static final CreateDataStreamAction INSTANCE = new CreateDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/create";

    private CreateDataStreamAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String name;
        private String timestampFieldName;

        public Request(String name) {
            this.name = name;
        }

        public void setTimestampFieldName(String timestampFieldName) {
            this.timestampFieldName = timestampFieldName;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (Strings.hasText(name) == false) {
                validationException = ValidateActions.addValidationError("name is missing", validationException);
            }
            if (Strings.hasText(timestampFieldName) == false) {
                validationException = ValidateActions.addValidationError("timestamp field name is missing", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.timestampFieldName = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeString(timestampFieldName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return name.equals(request.name) &&
                timestampFieldName.equals(request.timestampFieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, timestampFieldName);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final MetadataCreateIndexService metadataCreateIndexService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService metadataCreateIndexService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.metadataCreateIndexService = metadataCreateIndexService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected AcknowledgedResponse read(StreamInput in) throws IOException {
            return new AcknowledgedResponse(in);
        }

        @Override
        protected void masterOperation(Task task, Request request, ClusterState state,
                                       ActionListener<AcknowledgedResponse> listener) throws Exception {
            clusterService.submitStateUpdateTask("create-data-stream [" + request.name + "]",
                new ClusterStateUpdateTask(Priority.HIGH) {

                    @Override
                    public TimeValue timeout() {
                        return request.masterNodeTimeout();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        return createDataStream(metadataCreateIndexService, currentState, request);
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new AcknowledgedResponse(true));
                    }
                });
        }

        static ClusterState createDataStream(MetadataCreateIndexService metadataCreateIndexService,
                                             ClusterState currentState,
                                             Request request) throws Exception {
            if (currentState.metadata().dataStreams().containsKey(request.name)) {
                throw new IllegalArgumentException("data_stream [" + request.name + "] already exists");
            }

            MetadataCreateIndexService.validateIndexOrAliasName(request.name,
                (s1, s2) -> new IllegalArgumentException("data_stream [" + s1 + "] " + s2));

            if (request.name.toLowerCase(Locale.ROOT).equals(request.name) == false) {
                throw new IllegalArgumentException("data_stream [" + request.name + "] must be lowercase");
            }
            if (request.name.startsWith(".")) {
                throw new IllegalArgumentException("data_stream [" + request.name + "] must not start with '.'");
            }

            String firstBackingIndexName = DataStream.getBackingIndexName(request.name, 1);
            CreateIndexClusterStateUpdateRequest createIndexRequest =
                new CreateIndexClusterStateUpdateRequest("initialize_data_stream", firstBackingIndexName, firstBackingIndexName)
                .settings(Settings.builder().put("index.hidden", true).build());
            currentState = metadataCreateIndexService.applyCreateIndexRequest(currentState, createIndexRequest, false);
            IndexMetadata firstBackingIndex = currentState.metadata().index(firstBackingIndexName);
            assert firstBackingIndex != null;

            Metadata.Builder builder = Metadata.builder(currentState.metadata()).put(
                new DataStream(request.name, request.timestampFieldName, List.of(firstBackingIndex.getIndex())));
            logger.info("adding data stream [{}]", request.name);
            return ClusterState.builder(currentState).metadata(builder).build();
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}

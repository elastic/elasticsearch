/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateV2;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService;
import org.elasticsearch.cluster.metadata.MetadataCreateDataStreamService.CreateDataSteamClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class AutoCreateAction extends ActionType<AutoCreateAction.Response> {

    public static final AutoCreateAction INSTANCE = new AutoCreateAction();
    public static final String NAME = "indices:admin/auto_create";

    private AutoCreateAction() {
        super(NAME, Response::new);
    }

    public static class Request extends MasterNodeReadRequest<Request> {

        private final Set<String> names;
        private final Boolean preferV2Templates;
        private final TimeValue timeout;

        public Request(Set<String> names, Boolean preferV2Templates, TimeValue timeout) {
            this.names = Objects.requireNonNull(names);
            this.preferV2Templates = preferV2Templates;
            this.timeout = timeout;
            assert names.size() != 0;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readSet(StreamInput::readString);
            this.preferV2Templates = in.readOptionalBoolean();
            this.timeout = in.readTimeValue();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringCollection(names);
            out.writeOptionalBoolean(preferV2Templates);
            out.writeTimeValue(timeout);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return names.equals(request.names) &&
                Objects.equals(preferV2Templates, request.preferV2Templates) &&
                timeout.equals(request.timeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names, preferV2Templates, timeout);
        }
    }

    public static class Response extends ActionResponse {

        private final Map<String, Exception> failureByNames;

        public Response(Map<String, Exception> failureByNames) {
            this.failureByNames = failureByNames;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            failureByNames = in.readMap(StreamInput::readString, StreamInput::readException);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(failureByNames, StreamOutput::writeString, StreamOutput::writeException);
        }

        public Map<String, Exception> getFailureByNames() {
            return failureByNames;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            /*
             * Exception does not implement equals(...) so we will compute the hash code based on the key set and the
             * messages.
             */
            return Objects.equals(getKeys(failureByNames), getKeys(response.failureByNames)) &&
                Objects.equals(getExceptionMessages(failureByNames), getExceptionMessages(response.failureByNames));
        }

        @Override
        public int hashCode() {
            /*
             * Exception does not implement hash code so we will compute the hash code based on the key set and the
             * messages.
             */
            return Objects.hash(getKeys(failureByNames), getExceptionMessages(failureByNames));
        }

        private static List<String> getExceptionMessages(final Map<String, Exception> map) {
            return map.values().stream().map(Throwable::getMessage).sorted(String::compareTo).collect(Collectors.toList());
        }

        private static List<String> getKeys(final Map<String, Exception> map) {
            return map.keySet().stream().sorted(String::compareTo).collect(Collectors.toList());
        }
    }

    public static final class TransportAction extends TransportMasterNodeAction<Request, Response> {

        private final MetadataCreateIndexService createIndexService;
        private final MetadataCreateDataStreamService metadataCreateDataStreamService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataCreateIndexService createIndexService,
                               MetadataCreateDataStreamService metadataCreateDataStreamService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.createIndexService = createIndexService;
            this.metadataCreateDataStreamService = metadataCreateDataStreamService;
        }

        @Override
        protected String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        protected Response read(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void masterOperation(Task task,
                                       Request request,
                                       ClusterState state,
                                       ActionListener<Response> listener) throws Exception {
            // Should this be an AckedClusterStateUpdateTask and
            // should we add ActiveShardsObserver.waitForActiveShards(...) here?
            // (This api used from TransportBulkAction only and it currently ignores CreateIndexResponse, so
            // I think there is no need to include acked and shard ackeds here)
            clusterService.submitStateUpdateTask("auto create resources for [" + request.names + "]",
                new ClusterStateUpdateTask(Priority.HIGH) {

                    final NavigableMap<String, Exception> result = new TreeMap<>();

                    @Override
                    public TimeValue timeout() {
                        return request.timeout;
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(e);
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        Set<String> autoCreateIndices = new HashSet<>(request.names);
                        Map<String, IndexTemplateV2.DataStreamTemplate> autoCreateDataStreams =
                            resolveAutoCreateDataStreams(currentState.metadata(), autoCreateIndices);

                        for (Map.Entry<String, IndexTemplateV2.DataStreamTemplate> entry : autoCreateDataStreams.entrySet()) {
                            String dataStreamName = entry.getKey();
                            String timestampField = entry.getValue().getTimestampField();
                            // TODO: add reason
                            CreateDataSteamClusterStateUpdateRequest req =
                                new CreateDataSteamClusterStateUpdateRequest(dataStreamName, timestampField, request.timeout);
                            try {
                                currentState = metadataCreateDataStreamService.createDataStream(req, currentState);
                            } catch (Exception e) {
                                result.put(dataStreamName, e);
                            }
                        }

                        for (String indexName : autoCreateIndices) {
                            CreateIndexClusterStateUpdateRequest req = new CreateIndexClusterStateUpdateRequest("auto(bulk api)",
                                indexName, indexName).ackTimeout(request.timeout).masterNodeTimeout(request.masterNodeTimeout())
                                .preferV2Templates(request.preferV2Templates);
                            try {
                                currentState = createIndexService.applyCreateIndexRequest(currentState, req, false);
                            } catch (Exception e) {
                                result.put(indexName, e);
                            }
                        }

                        return currentState;
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        listener.onResponse(new AutoCreateAction.Response(result));
                    }
                });
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.names.toArray(new String[0]));
        }
    }

    static Map<String, IndexTemplateV2.DataStreamTemplate> resolveAutoCreateDataStreams(Metadata metadata,
                                                                                        Set<String> autoCreateIndices) {
        Map<String, IndexTemplateV2.DataStreamTemplate> autoCreateDataStreams = new HashMap<>();
        Iterator<String> autoCreateIndicesIterator = autoCreateIndices.iterator();
        while (autoCreateIndicesIterator.hasNext()) {
            String indexName = autoCreateIndicesIterator.next();
            String v2Template = MetadataIndexTemplateService.findV2Template(metadata, indexName, false);
            if (v2Template != null) {
                IndexTemplateV2 indexTemplateV2 = metadata.templatesV2().get(v2Template);
                if (indexTemplateV2.getDataStreamTemplate() != null) {
                    autoCreateIndicesIterator.remove();
                    autoCreateDataStreams.put(indexName, indexTemplateV2.getDataStreamTemplate());
                }
            }
        }
        return autoCreateDataStreams;
    }

}

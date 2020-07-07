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
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.SnapshotInProgressException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class DeleteDataStreamAction extends ActionType<AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(DeleteDataStreamAction.class);

    public static final DeleteDataStreamAction INSTANCE = new DeleteDataStreamAction();
    public static final String NAME = "indices:admin/data_stream/delete";

    private DeleteDataStreamAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    public static class Request extends MasterNodeRequest<Request> {

        private final String[] names;

        public Request(String[] names) {
            this.names = Objects.requireNonNull(names);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (CollectionUtils.isEmpty(names)) {
                validationException = addValidationError("no data stream(s) specified", validationException);
            }
            return validationException;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.names = in.readStringArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(names);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return Arrays.equals(names, request.names);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(names);
        }
    }

    public static class TransportAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

        private final MetadataDeleteIndexService deleteIndexService;

        @Inject
        public TransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               MetadataDeleteIndexService deleteIndexService) {
            super(NAME, transportService, clusterService, threadPool, actionFilters, Request::new, indexNameExpressionResolver);
            this.deleteIndexService = deleteIndexService;
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
        protected void masterOperation(Request request, ClusterState state,
                                       ActionListener<AcknowledgedResponse> listener) throws Exception {
            clusterService.submitStateUpdateTask("remove-data-stream [" + Strings.arrayToCommaDelimitedString(request.names) + "]",
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
                public ClusterState execute(ClusterState currentState) {
                    return removeDataStream(deleteIndexService, currentState, request);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(new AcknowledgedResponse(true));
                }
            });
        }

        static ClusterState removeDataStream(MetadataDeleteIndexService deleteIndexService, ClusterState currentState, Request request) {
            Set<String> dataStreams = new HashSet<>();
            Set<String> snapshottingDataStreams = new HashSet<>();
            for (String name : request.names) {
                for (String dataStreamName : currentState.metadata().dataStreams().keySet()) {
                    if (Regex.simpleMatch(name, dataStreamName)) {
                        dataStreams.add(dataStreamName);
                    }
                }

                snapshottingDataStreams.addAll(SnapshotsService.snapshottingDataStreams(currentState, dataStreams));
            }

            if (dataStreams.isEmpty()) {
                // if only a match-all pattern was specified and no data streams were found because none exist, do not
                // fail with data stream missing exception
                if (request.names.length == 1 && Regex.isMatchAllPattern(request.names[0])) {
                    return currentState;
                }
                throw new ResourceNotFoundException("data_streams matching [" + Strings.arrayToCommaDelimitedString(request.names) +
                    "] not found");
            }

            if (snapshottingDataStreams.isEmpty() == false) {
                throw new SnapshotInProgressException("Cannot delete data streams that are being snapshotted: " + snapshottingDataStreams +
                    ". Try again after snapshot finishes or cancel the currently running snapshot.");
            }

            Set<Index> backingIndicesToRemove = new HashSet<>();
            for (String dataStreamName : dataStreams) {
                DataStream dataStream = currentState.metadata().dataStreams().get(dataStreamName);
                assert dataStream != null;
                backingIndicesToRemove.addAll(dataStream.getIndices());
            }

            // first delete the data streams and then the indices:
            // (this to avoid data stream validation from failing when deleting an index that is part of a data stream
            // without updating the data stream)
            // TODO: change order when delete index api also updates the data stream the index to be removed is member of
            Metadata.Builder metadata = Metadata.builder(currentState.metadata());
            for (String ds : dataStreams) {
                logger.info("removing data stream [{}]", ds);
                metadata.removeDataStream(ds);
            }
            currentState = ClusterState.builder(currentState).metadata(metadata).build();
            return deleteIndexService.deleteIndices(currentState, backingIndicesToRemove);
        }

        @Override
        protected ClusterBlockException checkBlock(Request request, ClusterState state) {
            return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        }
    }

}

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

package org.elasticsearch.action.admin.indices.dangling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportDeleteDanglingIndexAction extends TransportMasterNodeAction<DeleteDanglingIndexRequest, DeleteDanglingIndexResponse> {
    private static final Logger logger = LogManager.getLogger(TransportDeleteDanglingIndexAction.class);

    private final Settings settings;

    @Inject
    public TransportDeleteDanglingIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Settings settings
    ) {
        super(
            DeleteDanglingIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            DeleteDanglingIndexRequest::new,
            indexNameExpressionResolver
        );
        this.settings = settings;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.WRITE;
    }

    @Override
    protected DeleteDanglingIndexResponse read(StreamInput in) throws IOException {
        return new DeleteDanglingIndexResponse(in);
    }

    @Override
    protected void masterOperation(
        Task task,
        DeleteDanglingIndexRequest request,
        ClusterState state,
        ActionListener<DeleteDanglingIndexResponse> listener
    ) throws Exception {
        IndexMetaData indexMetaDataToDelete;
        try {
            indexMetaDataToDelete = getIndexMetaDataToDelete(request);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final String indexName = indexMetaDataToDelete.getIndex().getName();

        final ActionListener<ClusterStateUpdateResponse> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                listener.onResponse(new DeleteDanglingIndexResponse());
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to delete dangling index [{}]" + indexName, e);
                listener.onFailure(e);
            }
        };

        // This is checked now so that
        if (request.isAcceptDataLoss() == false) {
            throw new IllegalArgumentException("accept_data_loss must be set to true");
        }

        this.clusterService.submitStateUpdateTask(
            "delete-dangling-index " + indexName,
            new AckedClusterStateUpdateTask<>(Priority.NORMAL, new DeleteIndexRequest(), actionListener) {

                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterState execute(final ClusterState currentState) {
                    return deleteDanglingIndex(currentState, indexMetaDataToDelete);
                }
            }
        );
    }

    private ClusterState deleteDanglingIndex(ClusterState currentState, IndexMetaData indexMetaDataToDelete) {
        final MetaData meta = currentState.metaData();

        MetaData.Builder metaDataBuilder = MetaData.builder(meta);

        final IndexGraveyard.Builder graveyardBuilder = IndexGraveyard.builder(metaDataBuilder.indexGraveyard());

        final IndexGraveyard newGraveyard = graveyardBuilder.addTombstone(indexMetaDataToDelete.getIndex()).build(settings);
        metaDataBuilder.indexGraveyard(newGraveyard);

        return ClusterState.builder(currentState).metaData(metaDataBuilder.build()).build();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDanglingIndexRequest request, ClusterState state) {
        return null;
    }

    private IndexMetaData getIndexMetaDataToDelete(DeleteDanglingIndexRequest request) {
        String indexUuid = request.getIndexUuid();

        if (indexUuid == null || indexUuid.isEmpty()) {
            throw new IllegalArgumentException("No index UUID specified in request");
        }

        List<IndexMetaData> matchingMetaData = new ArrayList<>();

        final List<NodeDanglingIndicesResponse> nodes = fetchDanglingIndices().actionGet().getNodes();

        for (NodeDanglingIndicesResponse response : nodes) {
            for (IndexMetaData danglingIndex : response.getDanglingIndices()) {
                if (danglingIndex.getIndexUUID().equals(indexUuid)) {
                    matchingMetaData.add(danglingIndex);
                }
            }
        }

        if (matchingMetaData.isEmpty()) {
            throw new IllegalArgumentException("No dangling index found for UUID [" + indexUuid + "]");
        }

        // Although we could find metadata for the same index on multiple nodes, we return the first
        // metadata here because only the index part goes into the graveyard, which is basically the
        // name and UUID.
        return matchingMetaData.get(0);
    }

    private ActionFuture<ListDanglingIndicesResponse> fetchDanglingIndices() {
        final PlainActionFuture<ListDanglingIndicesResponse> future = PlainActionFuture.newFuture();

        this.transportService.sendRequest(
            this.transportService.getLocalNode(),
            ListDanglingIndicesAction.NAME,
            new ListDanglingIndicesRequest(),
            new TransportResponseHandler<ListDanglingIndicesResponse>() {
                @Override
                public void handleResponse(ListDanglingIndicesResponse response) {
                    future.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    future.onFailure(exp);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public ListDanglingIndicesResponse read(StreamInput in) throws IOException {
                    return new ListDanglingIndicesResponse(in);
                }
            }
        );

        return future;
    }
}

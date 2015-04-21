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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataDeleteIndexService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportDeleteIndexAction extends TransportMasterNodeOperationAction<DeleteIndexRequest, DeleteIndexResponse> {

    private final MetaDataDeleteIndexService deleteIndexService;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetaDataDeleteIndexService deleteIndexService,
                                      NodeSettingsService nodeSettingsService, ActionFilters actionFilters) {
        super(settings, DeleteIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, DeleteIndexRequest.class);
        this.deleteIndexService = deleteIndexService;
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteIndexResponse newResponse() {
        return new DeleteIndexResponse();
    }

    @Override
    protected void doExecute(DeleteIndexRequest request, ActionListener<DeleteIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected void masterOperation(final DeleteIndexRequest request, final ClusterState state, final ActionListener<DeleteIndexResponse> listener) throws ElasticsearchException {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        if (concreteIndices.length == 0) {
            listener.onResponse(new DeleteIndexResponse(true));
            return;
        }
        // TODO: this API should be improved, currently, if one delete index failed, we send a failure, we should send a response array that includes all the indices that were deleted
        final CountDown count = new CountDown(concreteIndices.length);
        for (final String index : concreteIndices) {
            deleteIndexService.deleteIndex(new MetaDataDeleteIndexService.Request(index).timeout(request.timeout()).masterTimeout(request.masterNodeTimeout()), new MetaDataDeleteIndexService.Listener() {

                private volatile Throwable lastFailure;
                private volatile boolean ack = true;

                @Override
                public void onResponse(MetaDataDeleteIndexService.Response response) {
                    if (!response.acknowledged()) {
                        ack = false;
                    }
                    if (count.countDown()) {
                        if (lastFailure != null) {
                            listener.onFailure(lastFailure);
                        } else {
                            listener.onResponse(new DeleteIndexResponse(ack));
                        }
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.debug("[{}] failed to delete index", t, index);
                    lastFailure = t;
                    if (count.countDown()) {
                        listener.onFailure(t);
                    }
                }
            });
        }
    }
}

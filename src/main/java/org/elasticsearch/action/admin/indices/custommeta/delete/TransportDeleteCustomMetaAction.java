/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.indices.custommeta.delete;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.custom.SimpleCustomMetaData;
import org.elasticsearch.cluster.metadata.custom.SimpleCustomMetaMissingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Delete index custom meta.
 */
public class TransportDeleteCustomMetaAction extends TransportMasterNodeOperationAction<DeleteCustomMetaRequest, DeleteCustomMetaResponse> {

    @Inject
    public TransportDeleteCustomMetaAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return DeleteCustomMetaAction.NAME;
    }

    @Override
    protected DeleteCustomMetaRequest newRequest() {
        return new DeleteCustomMetaRequest();
    }

    @Override
    protected DeleteCustomMetaResponse newResponse() {
        return new DeleteCustomMetaResponse();
    }

    @Override
    protected void doExecute(DeleteCustomMetaRequest request, ActionListener<DeleteCustomMetaResponse> listener) {
        // update to concrete indices
        request.indices(clusterService.state().metaData().concreteIndices(request.indices()));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteCustomMetaRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected DeleteCustomMetaResponse masterOperation(final DeleteCustomMetaRequest request, ClusterState state) throws ElasticSearchException {
        final AtomicReference<Throwable> failureRef = new AtomicReference<Throwable>();
        final CountDownLatch latch = new CountDownLatch(1);

        clusterService.submitStateUpdateTask("delete_custom_meta [" + request.name() + "]", new ProcessedClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                MetaData.Builder mdBuilder = MetaData.builder().metaData(currentState.metaData());

                boolean globalFoundAtLeastOne = false;
                for (String index : request.indices()) {
                    IndexMetaData indexMetaData = currentState.metaData().index(index);
                    if (indexMetaData == null) {
                        throw new IndexMissingException(new Index(index));
                    }
                    SimpleCustomMetaData custom_meta = indexMetaData.custom(SimpleCustomMetaData.TYPE);
                    if (custom_meta != null) {
                        List<SimpleCustomMetaData.Entry> entries = Lists.newArrayList();
                        for (SimpleCustomMetaData.Entry entry : custom_meta.entries()) {
                            if (request.name() == null || Regex.simpleMatch(request.name(), entry.name())) {
                                globalFoundAtLeastOne = true;
                                // don't add it...
                            } else {
                                entries.add(entry);
                            }
                        }
                        // a change, update it...
                        if (entries.size() != custom_meta.entries().size()) {
                            custom_meta = new SimpleCustomMetaData(entries.toArray(new SimpleCustomMetaData.Entry[entries.size()]));
                            IndexMetaData.Builder indexBuilder = IndexMetaData.newIndexMetaDataBuilder(indexMetaData).putCustom(SimpleCustomMetaData.TYPE, custom_meta);
                            mdBuilder.put(indexBuilder);
                        }
                    }
                }

                if (!globalFoundAtLeastOne) {
                    if (request.name() == null) {
                        // full match, just return with no failure
                        return currentState;
                    }
                    throw new SimpleCustomMetaMissingException(request.name());
                }

                if (logger.isInfoEnabled()) {
                    for (String index : request.indices()) {
                        IndexMetaData indexMetaData = currentState.metaData().index(index);
                        if (indexMetaData == null) {
                            throw new IndexMissingException(new Index(index));
                        }
                        SimpleCustomMetaData custom_meta = indexMetaData.custom(SimpleCustomMetaData.TYPE);
                        if (custom_meta != null) {
                            for (SimpleCustomMetaData.Entry entry : custom_meta.entries()) {
                                if (Regex.simpleMatch(request.name(), entry.name())) {
                                    logger.info("[{}] delete custom meta [{}]", index, entry.name());
                                }
                            }
                        }
                    }
                }

                return ClusterState.builder().state(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState clusterState) {
                latch.countDown();
            }
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            failureRef.set(e);
        }

        if (failureRef.get() != null) {
            if (failureRef.get() instanceof ElasticSearchException) {
                throw (ElasticSearchException) failureRef.get();
            } else {
                throw new ElasticSearchException(failureRef.get().getMessage(), failureRef.get());
            }
        }

        return new DeleteCustomMetaResponse(true);
    }
}

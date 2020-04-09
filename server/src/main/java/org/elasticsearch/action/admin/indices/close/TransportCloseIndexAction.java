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

package org.elasticsearch.action.admin.indices.close;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * Close index action
 */
public class TransportCloseIndexAction extends TransportMasterNodeAction<CloseIndexRequest, CloseIndexResponse> {

    private static final Logger logger = LogManager.getLogger(TransportCloseIndexAction.class);

    private final MetaDataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;
    private volatile boolean closeIndexEnabled;
    public static final Setting<Boolean> CLUSTER_INDICES_CLOSE_ENABLE_SETTING =
        Setting.boolSetting("cluster.indices.close.enable", true, Property.Dynamic, Property.NodeScope);

    @Inject
    public TransportCloseIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataIndexStateService indexStateService,
                                     ClusterSettings clusterSettings, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, DestructiveOperations destructiveOperations) {
        super(CloseIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, CloseIndexRequest::new,
            indexNameExpressionResolver);
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
        this.closeIndexEnabled = CLUSTER_INDICES_CLOSE_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_INDICES_CLOSE_ENABLE_SETTING, this::setCloseIndexEnabled);
    }

    private void setCloseIndexEnabled(boolean closeIndexEnabled) {
        this.closeIndexEnabled = closeIndexEnabled;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CloseIndexResponse read(StreamInput in) throws IOException {
        return new CloseIndexResponse(in);
    }

    @Override
    protected void doExecute(Task task, CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        if (closeIndexEnabled == false) {
            throw new IllegalStateException("closing indices is disabled - set [" + CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey() +
                ": true] to enable it. NOTE: closed indices still consume a significant amount of diskspace");
        }
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(final Task task,
                                   final CloseIndexRequest request,
                                   final ClusterState state,
                                   final ActionListener<CloseIndexResponse> listener) throws Exception {
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new CloseIndexResponse(true, false, Collections.emptyList()));
            return;
        }

        final CloseIndexClusterStateUpdateRequest closeRequest = new CloseIndexClusterStateUpdateRequest(task.getId())
            .ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .waitForActiveShards(request.waitForActiveShards())
            .indices(concreteIndices);
        indexStateService.closeIndices(closeRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to close indices [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }
}

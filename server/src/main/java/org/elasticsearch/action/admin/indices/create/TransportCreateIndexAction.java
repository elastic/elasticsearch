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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Set;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {

    private final MetadataCreateIndexService createIndexService;
    private final SystemIndices systemIndices;

    @Inject
    public TransportCreateIndexAction(TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, MetadataCreateIndexService createIndexService,
                                      ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                      SystemIndices systemIndices) {
        super(CreateIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, CreateIndexRequest::new,
            indexNameExpressionResolver, CreateIndexResponse::new, ThreadPool.Names.SAME);
        this.createIndexService = createIndexService;
        this.systemIndices = systemIndices;
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void masterOperation(Task task, final CreateIndexRequest request, final ClusterState state,
                                   final ActionListener<CreateIndexResponse> listener) {
        String cause = request.cause();
        if (cause.length() == 0) {
            cause = "api";
        }

        final String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.index());

        String mappings = request.mappings();
        Settings settings = request.settings();
        Set<Alias> aliases = request.aliases();

        String concreteIndexName = indexName;
        boolean isSystemIndex = false;

        SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(indexName);

        if (descriptor != null && descriptor.isAutomaticallyManaged()) {
            isSystemIndex = true;
            // System indices define their own settings and mappings, which cannot be overridden.
            mappings = descriptor.getMappings();
            settings = descriptor.getSettings();

            if (descriptor.getAliasName() == null) {
                aliases = Set.of();
            } else {
                concreteIndexName = descriptor.getIndexPattern();
                aliases = Set.of(new Alias(descriptor.getAliasName()));
            }
        }

        final CreateIndexClusterStateUpdateRequest updateRequest =
            new CreateIndexClusterStateUpdateRequest(cause, concreteIndexName, request.index())
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .aliases(aliases)
                .waitForActiveShards(request.waitForActiveShards());

        if (isSystemIndex) {
            updateRequest.waitForActiveShards(ActiveShardCount.ALL);
        }

        if (mappings != null) {
            updateRequest.mappings(mappings);
        }

        if (settings != null) {
            updateRequest.settings(settings);
        }

        createIndexService.createIndex(updateRequest, ActionListener.map(listener, response ->
            new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)));
    }

}

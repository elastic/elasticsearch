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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Objects;
import java.util.Set;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {
    private static final Logger logger = LogManager.getLogger(TransportCreateIndexAction.class);

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
        if (cause.isEmpty()) {
            cause = "api";
        }

        final String indexName = indexNameExpressionResolver.resolveDateMathExpression(request.index());

        final SystemIndexDescriptor descriptor = systemIndices.findMatchingDescriptor(indexName);
        final boolean isSystemIndex = descriptor != null && descriptor.isAutomaticallyManaged();

        final CreateIndexClusterStateUpdateRequest updateRequest;

        // Requests that a cluster generates itself are permitted to create a system index with
        // different mappings, settings etc. This is so that rolling upgrade scenarios still work.
        // We check this via the request's origin. Eventually, `SystemIndexManager` will reconfigure
        // the index to the latest settings.
        if (isSystemIndex && Strings.isNullOrEmpty(request.origin())) {
            final String message = descriptor.checkMinimumNodeVersion("create index", state.nodes().getMinNodeVersion());
            if (message != null) {
                logger.warn(message);
                listener.onFailure(new IllegalStateException(message));
                return;
            }
            updateRequest = buildSystemIndexUpdateRequest(request, cause, descriptor);
        } else {
            updateRequest = buildUpdateRequest(request, cause, indexName);
        }

        createIndexService.createIndex(updateRequest, listener.map(response ->
            new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)));
    }

    private CreateIndexClusterStateUpdateRequest buildUpdateRequest(CreateIndexRequest request, String cause, String indexName) {
        return new CreateIndexClusterStateUpdateRequest(cause, indexName, request.index()).ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .settings(request.settings())
            .mappings(request.mappings())
            .aliases(request.aliases())
            .waitForActiveShards(request.waitForActiveShards());
    }

    private CreateIndexClusterStateUpdateRequest buildSystemIndexUpdateRequest(
        CreateIndexRequest request,
        String cause,
        SystemIndexDescriptor descriptor
    ) {
        final Settings settings = Objects.requireNonNullElse(descriptor.getSettings(), Settings.EMPTY);

        final Set<Alias> aliases;
        if (descriptor.getAliasName() == null) {
            aliases = Set.of();
        } else {
            aliases = Set.of(new Alias(descriptor.getAliasName()));
        }

        final CreateIndexClusterStateUpdateRequest updateRequest = new CreateIndexClusterStateUpdateRequest(
            cause,
            descriptor.getPrimaryIndex(),
            request.index()
        );

        return updateRequest.ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .aliases(aliases)
            .waitForActiveShards(ActiveShardCount.ALL)
            .mappings(descriptor.getMappings())
            .settings(settings);
    }
}

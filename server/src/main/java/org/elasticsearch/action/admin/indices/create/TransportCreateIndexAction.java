/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.core.List;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Set;

import static java.util.Collections.singletonMap;

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
    protected void masterOperation(final CreateIndexRequest request, final ClusterState state,
                                   final ActionListener<CreateIndexResponse> listener) {
        String cause = request.cause();
        if (cause.isEmpty()) {
            cause = "api";
        }


        final long resolvedAt = System.currentTimeMillis();
        final String indexName  = indexNameExpressionResolver.resolveDateMathExpression(request.index(), resolvedAt);

        final SystemIndexDescriptor mainDescriptor = systemIndices.findMatchingDescriptor(indexName);
        final boolean isSystemIndex = mainDescriptor != null && mainDescriptor.isAutomaticallyManaged();
        if (mainDescriptor != null && mainDescriptor.isNetNew()) {
            final SystemIndexAccessLevel systemIndexAccessLevel = systemIndices.getSystemIndexAccessLevel(threadPool.getThreadContext());
            if (systemIndexAccessLevel != SystemIndexAccessLevel.ALL) {
                if (systemIndexAccessLevel == SystemIndexAccessLevel.RESTRICTED) {
                    if (systemIndices.getProductSystemIndexNamePredicate(threadPool.getThreadContext()).test(indexName) == false) {
                        throw systemIndices.netNewSystemIndexAccessException(threadPool.getThreadContext(), List.of(indexName));
                    }
                } else {
                    // BACKWARDS_COMPATIBLE_ONLY should never be a possibility here, it cannot be returned from getSystemIndexAccessLevel
                    assert systemIndexAccessLevel == SystemIndexAccessLevel.NONE :
                        "Expected no system index access but level is " + systemIndexAccessLevel;
                    throw systemIndices.netNewSystemIndexAccessException(threadPool.getThreadContext(), List.of(indexName));
                }
            }
        }

        final CreateIndexClusterStateUpdateRequest updateRequest;

        // Requests that a cluster generates itself are permitted to create a system index with
        // different mappings, settings etc. This is so that rolling upgrade scenarios still work.
        // We check this via the request's origin. Eventually, `SystemIndexManager` will reconfigure
        // the index to the latest settings.
        if (isSystemIndex && Strings.isNullOrEmpty(request.origin())) {
            final SystemIndexDescriptor descriptor =
                mainDescriptor.getDescriptorCompatibleWith(state.nodes().getSmallestNonClientNodeVersion());
            if (descriptor == null) {
                final String message = mainDescriptor.getMinimumNodeVersionMessage("create index");
                logger.warn(message);
                listener.onFailure(new IllegalStateException(message));
                return;
            }
            updateRequest = buildSystemIndexUpdateRequest(request, cause, descriptor);
        } else {
            updateRequest = buildUpdateRequest(request, cause, indexName, resolvedAt);
        }

        createIndexService.createIndex(updateRequest, listener.map(response ->
            new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName)));
    }

    private CreateIndexClusterStateUpdateRequest buildUpdateRequest(CreateIndexRequest request, String cause,
                                                                    String indexName, long nameResolvedAt) {
        return new CreateIndexClusterStateUpdateRequest(cause, indexName, request.index()).ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .settings(request.settings())
            .mappings(request.mappings())
            .aliases(request.aliases())
            .nameResolvedInstant(nameResolvedAt)
            .waitForActiveShards(request.waitForActiveShards());
    }

    private CreateIndexClusterStateUpdateRequest buildSystemIndexUpdateRequest(
        CreateIndexRequest request,
        String cause,
        SystemIndexDescriptor descriptor
    ) {
        Settings settings = descriptor.getSettings();
        if (settings == null) {
            settings = Settings.EMPTY;
        }

        final Set<Alias> aliases;
        if (descriptor.getAliasName() == null) {
            aliases = Collections.emptySet();
        } else {
            aliases = Collections.singleton(new Alias(descriptor.getAliasName()));
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
            .mappings(singletonMap(descriptor.getIndexType(), descriptor.getMappings()))
            .settings(settings);
    }
}

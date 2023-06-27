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
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;

/**
 * Create index action.
 */
public class TransportCreateIndexAction extends TransportMasterNodeAction<CreateIndexRequest, CreateIndexResponse> {
    private static final Logger logger = LogManager.getLogger(TransportCreateIndexAction.class);

    private final MetadataCreateIndexService createIndexService;
    private final SystemIndices systemIndices;
    private final SettingsFilter settingsFilter;

    @Inject
    public TransportCreateIndexAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataCreateIndexService createIndexService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices,
        SettingsFilter settingsFilter
    ) {
        super(
            CreateIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CreateIndexRequest::new,
            indexNameExpressionResolver,
            CreateIndexResponse::new,
            ThreadPool.Names.SAME
        );
        this.createIndexService = createIndexService;
        this.systemIndices = systemIndices;
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected ClusterBlockException checkBlock(CreateIndexRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.index());
    }

    @Override
    protected void masterOperation(
        Task task,
        final CreateIndexRequest request,
        final ClusterState state,
        final ActionListener<CreateIndexResponse> listener
    ) {
        String cause = request.cause();
        if (cause.isEmpty()) {
            cause = "api";
        }

        final long resolvedAt = System.currentTimeMillis();
        final String indexName = IndexNameExpressionResolver.resolveDateMathExpression(request.index(), resolvedAt);

        final SystemIndexDescriptor mainDescriptor = systemIndices.findMatchingDescriptor(indexName);
        final boolean isSystemIndex = mainDescriptor != null;
        final boolean isManagedSystemIndex = isSystemIndex && mainDescriptor.isAutomaticallyManaged();
        if (mainDescriptor != null && mainDescriptor.isNetNew()) {
            final SystemIndexAccessLevel systemIndexAccessLevel = SystemIndices.getSystemIndexAccessLevel(threadPool.getThreadContext());
            if (systemIndexAccessLevel != SystemIndexAccessLevel.ALL) {
                if (systemIndexAccessLevel == SystemIndexAccessLevel.RESTRICTED) {
                    if (systemIndices.getProductSystemIndexNamePredicate(threadPool.getThreadContext()).test(indexName) == false) {
                        throw SystemIndices.netNewSystemIndexAccessException(threadPool.getThreadContext(), List.of(indexName));
                    }
                } else {
                    // BACKWARDS_COMPATIBLE_ONLY should never be a possibility here, it cannot be returned from getSystemIndexAccessLevel
                    assert systemIndexAccessLevel == SystemIndexAccessLevel.NONE
                        : "Expected no system index access but level is " + systemIndexAccessLevel;
                    throw SystemIndices.netNewSystemIndexAccessException(threadPool.getThreadContext(), List.of(indexName));
                }
            }
        }

        if (isSystemIndex) {
            if (Objects.isNull(request.settings())) {
                request.settings(SystemIndexDescriptor.DEFAULT_SETTINGS);
            } else if (false == request.settings().hasValue(SETTING_INDEX_HIDDEN)) {
                request.settings(Settings.builder().put(request.settings()).put(SETTING_INDEX_HIDDEN, true).build());
            } else if (Boolean.FALSE.toString().equalsIgnoreCase(request.settings().get(SETTING_INDEX_HIDDEN))) {
                final String message = "Cannot create system index [" + indexName + "] with [index.hidden] set to 'false'";
                logger.warn(message);
                listener.onFailure(new IllegalStateException(message));
                return;
            }
        }

        try {
            settingsFilter.validateSettings(request.settings());
        } catch (Exception e) {
            listener.onFailure(e);
        }

        final CreateIndexClusterStateUpdateRequest updateRequest;

        // Requests that a cluster generates itself are permitted to create a system index with
        // different mappings, settings etc. This is so that rolling upgrade scenarios still work.
        // We check this via the request's origin. Eventually, `SystemIndexManager` will reconfigure
        // the index to the latest settings.
        if (isManagedSystemIndex && Strings.isNullOrEmpty(request.origin())) {
            final SystemIndexDescriptor descriptor = mainDescriptor.getDescriptorCompatibleWith(
                state.nodes().getSmallestNonClientNodeVersion()
            );
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

        createIndexService.createIndex(
            updateRequest,
            listener.map(response -> new CreateIndexResponse(response.isAcknowledged(), response.isShardsAcknowledged(), indexName))
        );
    }

    private CreateIndexClusterStateUpdateRequest buildUpdateRequest(
        CreateIndexRequest request,
        String cause,
        String indexName,
        long nameResolvedAt
    ) {
        Set<Alias> aliases = request.aliases().stream().peek(alias -> {
            if (systemIndices.isSystemName(alias.name())) {
                alias.isHidden(true);
            }
        }).collect(Collectors.toSet());
        return new CreateIndexClusterStateUpdateRequest(cause, indexName, request.index()).ackTimeout(request.timeout())
            .masterNodeTimeout(request.masterNodeTimeout())
            .settings(request.settings())
            .mappings(request.mappings())
            .aliases(aliases)
            .nameResolvedInstant(nameResolvedAt)
            .waitForActiveShards(request.waitForActiveShards());
    }

    private static CreateIndexClusterStateUpdateRequest buildSystemIndexUpdateRequest(
        CreateIndexRequest request,
        String cause,
        SystemIndexDescriptor descriptor
    ) {
        final Settings settings = Objects.requireNonNullElse(descriptor.getSettings(), Settings.EMPTY);

        final Set<Alias> aliases;
        if (descriptor.getAliasName() == null) {
            aliases = Set.of();
        } else {
            aliases = Set.of(new Alias(descriptor.getAliasName()).isHidden(true).writeIndex(true));
        }

        // Throw an error if we are trying to directly create a system index other than the primary system index (or the alias)
        if (request.index().equals(descriptor.getPrimaryIndex()) == false && request.index().equals(descriptor.getAliasName()) == false) {
            throw new IllegalArgumentException(
                "Cannot create system index with name " + request.index() + "; descriptor primary index is " + descriptor.getPrimaryIndex()
            );
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

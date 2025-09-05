/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.local.TransportLocalProjectMetadataAction;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.Transports;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class TransportGetAliasesAction extends TransportLocalProjectMetadataAction<GetAliasesRequest, GetAliasesResponse> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportGetAliasesAction.class);

    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SystemIndices systemIndices;
    private final ThreadContext threadContext;

    @Inject
    public TransportGetAliasesAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ProjectResolver projectResolver,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetAliasesAction.NAME,
            actionFilters,
            transportService.getTaskManager(),
            clusterService,
            clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT),
            projectResolver
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.systemIndices = systemIndices;
        this.threadContext = clusterService.threadPool().getThreadContext();
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ProjectState state) {
        // Resolve with system index access since we're just checking blocks
        return state.blocks()
            .indicesBlockedException(
                state.projectId(),
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state.metadata(), request)
            );
    }

    @Override
    protected void localClusterStateOperation(
        Task task,
        GetAliasesRequest request,
        ProjectState state,
        ActionListener<GetAliasesResponse> listener
    ) {
        assert Transports.assertNotTransportThread("no need to avoid the context switch and may be expensive if there are many aliases");
        final var cancellableTask = (CancellableTask) task;
        // resolve all concrete indices upfront and warn/error later
        final ProjectMetadata projectMetadata = state.metadata();
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(projectMetadata, request);
        final SystemIndexAccessLevel systemIndexAccessLevel = indexNameExpressionResolver.getSystemIndexAccessLevel();
        Map<String, List<AliasMetadata>> aliases = projectMetadata.findAliases(request.aliases(), concreteIndices);
        cancellableTask.ensureNotCancelled();
        listener.onResponse(
            new GetAliasesResponse(
                postProcess(request, concreteIndices, aliases, projectMetadata, systemIndexAccessLevel, threadContext, systemIndices),
                postProcess(indexNameExpressionResolver, request, projectMetadata)
            )
        );
    }

    /**
     * Fills alias result with empty entries for requested indices when no specific aliases were requested.
     */
    static Map<String, List<AliasMetadata>> postProcess(
        GetAliasesRequest request,
        String[] concreteIndices,
        Map<String, List<AliasMetadata>> aliases,
        ProjectMetadata projectMetadata,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext,
        SystemIndices systemIndices
    ) {
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        Map<String, List<AliasMetadata>> mapBuilder = new HashMap<>(aliases);
        for (String index : concreteIndices) {
            IndexAbstraction ia = projectMetadata.getIndicesLookup().get(index);
            assert ia.getType() == IndexAbstraction.Type.CONCRETE_INDEX;
            if (ia.getParentDataStream() != null) {
                // Don't include backing indices of data streams,
                // because it is just noise. Aliases can't refer
                // to backing indices directly.
                continue;
            }

            if (aliases.get(index) == null && noAliasesSpecified) {
                List<AliasMetadata> previous = mapBuilder.put(index, Collections.emptyList());
                assert previous == null;
            }
        }
        final Map<String, List<AliasMetadata>> finalResponse = Collections.unmodifiableMap(mapBuilder);
        if (systemIndexAccessLevel != SystemIndexAccessLevel.ALL) {
            checkSystemIndexAccess(systemIndices, projectMetadata, finalResponse, systemIndexAccessLevel, threadContext);
        }
        return finalResponse;
    }

    static Map<String, List<DataStreamAlias>> postProcess(
        IndexNameExpressionResolver resolver,
        GetAliasesRequest request,
        ProjectMetadata projectMetadata
    ) {
        Map<String, List<DataStreamAlias>> result = new HashMap<>();
        List<String> requestedDataStreams = resolver.dataStreamNames(projectMetadata, request.indicesOptions(), request.indices());

        return projectMetadata.findDataStreamAliases(request.aliases(), requestedDataStreams.toArray(new String[0]));
    }

    private static void checkSystemIndexAccess(
        SystemIndices systemIndices,
        ProjectMetadata projectMetadata,
        Map<String, List<AliasMetadata>> aliasesMap,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext
    ) {
        final Predicate<String> systemIndexAccessAllowPredicate;
        if (systemIndexAccessLevel == SystemIndexAccessLevel.NONE) {
            systemIndexAccessAllowPredicate = Predicates.never();
        } else if (systemIndexAccessLevel == SystemIndexAccessLevel.RESTRICTED) {
            systemIndexAccessAllowPredicate = systemIndices.getProductSystemIndexNamePredicate(threadContext);
        } else {
            throw new IllegalArgumentException("Unexpected system index access level: " + systemIndexAccessLevel);
        }

        List<String> netNewSystemIndices = new ArrayList<>();
        List<String> systemIndicesNames = new ArrayList<>();
        aliasesMap.keySet().forEach(indexName -> {
            IndexMetadata index = projectMetadata.index(indexName);
            if (index != null && index.isSystem()) {
                if (systemIndexAccessAllowPredicate.test(indexName) == false) {
                    if (systemIndices.isNetNewSystemIndex(indexName)) {
                        netNewSystemIndices.add(indexName);
                    } else {
                        systemIndicesNames.add(indexName);
                    }
                }
            }
        });
        if (systemIndicesNames.isEmpty() == false) {
            deprecationLogger.warn(
                DeprecationCategory.API,
                "open_system_index_access",
                "this request accesses system indices: {}, but in a future major version, direct access to system "
                    + "indices will be prevented by default",
                systemIndicesNames
            );
        }
        if (netNewSystemIndices.isEmpty() == false) {
            throw SystemIndices.netNewSystemIndexAccessException(threadContext, netNewSystemIndices);
        }
    }
}

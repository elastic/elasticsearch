/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.SystemIndices.SystemIndexAccessLevel;
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

public class TransportGetAliasesAction extends TransportMasterNodeReadAction<GetAliasesRequest, GetAliasesResponse> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportGetAliasesAction.class);

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetAliasesAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SystemIndices systemIndices
    ) {
        super(
            GetAliasesAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            GetAliasesRequest::new,
            indexNameExpressionResolver,
            GetAliasesResponse::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.systemIndices = systemIndices;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        // Resolve with system index access since we're just checking blocks
        return state.blocks()
            .indicesBlockedException(
                ClusterBlockLevel.METADATA_READ,
                indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request)
            );
    }

    @Override
    protected void masterOperation(Task task, GetAliasesRequest request, ClusterState state, ActionListener<GetAliasesResponse> listener) {
        assert Transports.assertNotTransportThread("no need to avoid the context switch and may be expensive if there are many aliases");
        // resolve all concrete indices upfront and warn/error later
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request);
        final SystemIndexAccessLevel systemIndexAccessLevel = indexNameExpressionResolver.getSystemIndexAccessLevel();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = state.metadata().findAliases(request.aliases(), concreteIndices);
        listener.onResponse(
            new GetAliasesResponse(
                postProcess(request, concreteIndices, aliases, state, systemIndexAccessLevel, threadPool.getThreadContext(), systemIndices),
                postProcess(indexNameExpressionResolver, request, state)
            )
        );
    }

    /**
     * Fills alias result with empty entries for requested indices when no specific aliases were requested.
     */
    static ImmutableOpenMap<String, List<AliasMetadata>> postProcess(
        GetAliasesRequest request,
        String[] concreteIndices,
        ImmutableOpenMap<String, List<AliasMetadata>> aliases,
        ClusterState state,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext,
        SystemIndices systemIndices
    ) {
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder(aliases);
        for (String index : concreteIndices) {
            IndexAbstraction ia = state.metadata().getIndicesLookup().get(index);
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
        final ImmutableOpenMap<String, List<AliasMetadata>> finalResponse = mapBuilder.build();
        if (systemIndexAccessLevel != SystemIndexAccessLevel.ALL) {
            checkSystemIndexAccess(request, systemIndices, state, finalResponse, systemIndexAccessLevel, threadContext);
        }
        return finalResponse;
    }

    static Map<String, List<DataStreamAlias>> postProcess(
        IndexNameExpressionResolver resolver,
        GetAliasesRequest request,
        ClusterState state
    ) {
        Map<String, List<DataStreamAlias>> result = new HashMap<>();
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        List<String> requestedDataStreams = resolver.dataStreamNames(state, request.indicesOptions(), request.indices());
        for (String requestedDataStream : requestedDataStreams) {
            List<DataStreamAlias> aliases = state.metadata()
                .dataStreamAliases()
                .values()
                .stream()
                .filter(alias -> alias.getDataStreams().contains(requestedDataStream))
                .filter(alias -> noAliasesSpecified || Regex.simpleMatch(request.aliases(), alias.getName()))
                .toList();
            if (aliases.isEmpty() == false) {
                result.put(requestedDataStream, aliases);
            }
        }
        return result;
    }

    private static void checkSystemIndexAccess(
        GetAliasesRequest request,
        SystemIndices systemIndices,
        ClusterState state,
        ImmutableOpenMap<String, List<AliasMetadata>> aliasesMap,
        SystemIndexAccessLevel systemIndexAccessLevel,
        ThreadContext threadContext
    ) {
        final Predicate<IndexMetadata> systemIndexAccessAllowPredicate;
        if (systemIndexAccessLevel == SystemIndexAccessLevel.NONE) {
            systemIndexAccessAllowPredicate = indexMetadata -> false;
        } else if (systemIndexAccessLevel == SystemIndexAccessLevel.RESTRICTED) {
            systemIndexAccessAllowPredicate = systemIndices.getProductSystemIndexMetadataPredicate(threadContext);
        } else {
            throw new IllegalArgumentException("Unexpected system index access level: " + systemIndexAccessLevel);
        }

        List<String> netNewSystemIndices = new ArrayList<>();
        List<String> systemIndicesNames = new ArrayList<>();
        aliasesMap.keySet().forEach(indexName -> {
            IndexMetadata index = state.metadata().index(indexName);
            if (index != null && index.isSystem()) {
                if (systemIndexAccessAllowPredicate.test(index) == false) {
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

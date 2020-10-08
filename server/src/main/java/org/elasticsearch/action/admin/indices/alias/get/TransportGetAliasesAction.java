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
package org.elasticsearch.action.admin.indices.alias.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class TransportGetAliasesAction extends TransportMasterNodeReadAction<GetAliasesRequest, GetAliasesResponse> {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransportGetAliasesAction.class);

    private final SystemIndices systemIndices;

    @Inject
    public TransportGetAliasesAction(TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, ActionFilters actionFilters,
                                     IndexNameExpressionResolver indexNameExpressionResolver, SystemIndices systemIndices) {
        super(GetAliasesAction.NAME, transportService, clusterService, threadPool, actionFilters, GetAliasesRequest::new,
            indexNameExpressionResolver);
        this.systemIndices = systemIndices;
    }

    @Override
    protected String executor() {
        // very lightweight operation all in memory no need to fork to a thread pool
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        // Resolve with system index access since we're just checking blocks
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNamesWithSystemIndexAccess(state, request));
    }

    @Override
    protected GetAliasesResponse read(StreamInput in) throws IOException {
        return new GetAliasesResponse(in);
    }

    @Override
    protected void masterOperation(Task task, GetAliasesRequest request, ClusterState state, ActionListener<GetAliasesResponse> listener) {
        String[] concreteIndices;
        // Switch to a context which will drop any deprecation warnings, because there may be indices resolved here which are not
        // returned in the final response. We'll add warnings back later if necessary in checkSystemIndexAccess.
        try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().newStoredContext(false)) {
            concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        }
        final boolean systemIndexAccessAllowed = indexNameExpressionResolver.isSystemIndexAccessAllowed();
        ImmutableOpenMap<String, List<AliasMetadata>> aliases = state.metadata().findAliases(request, concreteIndices);
        listener.onResponse(new GetAliasesResponse(postProcess(request, concreteIndices, aliases, state,
            systemIndexAccessAllowed, systemIndices)));
    }

    /**
     * Fills alias result with empty entries for requested indices when no specific aliases were requested.
     */
    static ImmutableOpenMap<String, List<AliasMetadata>> postProcess(GetAliasesRequest request, String[] concreteIndices,
                                                                     ImmutableOpenMap<String, List<AliasMetadata>> aliases,
                                                                     ClusterState state, boolean systemIndexAccessAllowed,
                                                                     SystemIndices systemIndices) {
        boolean noAliasesSpecified = request.getOriginalAliases() == null || request.getOriginalAliases().length == 0;
        ImmutableOpenMap.Builder<String, List<AliasMetadata>> mapBuilder = ImmutableOpenMap.builder(aliases);
        for (String index : concreteIndices) {
            if (aliases.get(index) == null && noAliasesSpecified) {
                List<AliasMetadata> previous = mapBuilder.put(index, Collections.emptyList());
                assert previous == null;
            }
        }
        final ImmutableOpenMap<String, List<AliasMetadata>> finalResponse = mapBuilder.build();
        if (systemIndexAccessAllowed == false) {
            checkSystemIndexAccess(request, systemIndices, state, finalResponse);
        }
        return finalResponse;
    }

    private static void checkSystemIndexAccess(GetAliasesRequest request, SystemIndices systemIndices, ClusterState state,
                                               ImmutableOpenMap<String, List<AliasMetadata>> aliasesMap) {
        List<String> systemIndicesNames = new ArrayList<>();
        for (Iterator<String> it = aliasesMap.keysIt(); it.hasNext(); ) {
            String indexName = it.next();
            IndexMetadata index = state.metadata().index(indexName);
            if (index != null && index.isSystem()) {
                systemIndicesNames.add(indexName);
            }
        }
        if (systemIndicesNames.isEmpty() == false) {
            deprecationLogger.deprecate("open_system_index_access",
                "this request accesses system indices: {}, but in a future major version, direct access to system " +
                    "indices will be prevented by default", systemIndicesNames);
        } else {
            checkSystemAliasAccess(request, systemIndices);
        }
    }

    private static void checkSystemAliasAccess(GetAliasesRequest request, SystemIndices systemIndices) {
        final List<String> systemAliases = Arrays.stream(request.aliases())
            .filter(alias -> systemIndices.isSystemIndex(alias))
            .collect(Collectors.toList());
        if (systemAliases.isEmpty() == false) {
            deprecationLogger.deprecate("open_system_alias_access",
                "this request accesses aliases with names reserved for system indices: {}, but in a future major version, direct" +
                    "access to system indices and their aliases will not be allowed", systemAliases);
        }
    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskListener;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SystemIndexClient extends NodeClient {
    private NodeClient in;
    private final Collection<SystemIndexDescriptor> ownedSystemIndices;
    private final IndexNameExpressionResolver indexResolver;
    private final Supplier<ClusterState> clusterStateSupplier;

    public SystemIndexClient(NodeClient in, Collection<SystemIndexDescriptor> ownedSystemIndices,
                             Supplier<ClusterState> clusterStateSupplier, IndexNameExpressionResolver indexResolver) {
        super(in.settings(), in.threadPool());
        this.in = in;
        this.ownedSystemIndices = ownedSystemIndices;
        this.indexResolver = indexResolver;
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse>
    void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        // G-> Fix this so it actually works correctly instead of for like 80% of cases. See how Security does this
        try {
            if (request instanceof IndicesRequest.Replaceable) {
                resolveIndices((IndicesRequest.Replaceable) request);
            }
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        in.execute(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                                                                                                Request request,
                                                                                                ActionListener<Response> listener) {
        if (request instanceof IndicesRequest.Replaceable) {
            resolveIndices((IndicesRequest.Replaceable) request);
        }
        return in.executeLocally(action, request, listener);
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(ActionType<Response> action,
                                                                                                Request request,
                                                                                                TaskListener<Response> listener) {
        if (request instanceof IndicesRequest.Replaceable) {
            resolveIndices((IndicesRequest.Replaceable) request);
        }
        return in.executeLocally(action, request, listener);
    }

    @Override
    public void initialize(Map<ActionType, TransportAction> actions, TaskManager taskManager, Supplier<String> localNodeId,
                           RemoteClusterService remoteClusterService) {
        // Don't call super.initialize, just delegate the methods that use these.
        in.initialize(actions, taskManager, localNodeId, remoteClusterService);
    }

    @Override
    public void close() {
        super.close();
        in.close();
    }

    @Override
    public String getLocalNodeId() {
        return in.getLocalNodeId();
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return in.getRemoteClusterClient(clusterAlias);
    }

    private void resolveIndices(IndicesRequest.Replaceable request) {
        ClusterState state = clusterStateSupplier.get();

        // Tweak the indices options first, so the resolver can resolve system indices.
        IndicesOptions originalOptions = request.indicesOptions();
        EnumSet<IndicesOptions.Option> options = originalOptions.getOptions();
        options.add(IndicesOptions.Option.ALLOW_SYSTEM_INDEX_ACCESS);
        request.indicesOptions(new IndicesOptions(options, request.indicesOptions().getExpandWildcards()));

        List<Index> indices = Arrays.asList(
            indexResolver.concreteIndices(state, request, IndexNameExpressionResolver.Context.SystemIndexStrategy.INCLUDE)
        );

        if (indices.stream().anyMatch(index -> isSystemIndex(index, state))) {
            List<Index> nonOwnedSystemIndices = indices.stream()
                .filter(i -> isSystemIndex(i, state))
                .filter(i -> isOwnedSystemIndex(i) == false)
                .collect(Collectors.toList());
            if (nonOwnedSystemIndices.isEmpty() == false) {
                throw new IllegalArgumentException("attempted to access system indices which this plugin is not authorized for: "
                    + nonOwnedSystemIndices);
            }

            String[] finalIndices = indices.stream()
                .map(Index::getName)
                .collect(Collectors.toList())
                .toArray(Strings.EMPTY_ARRAY);
            request.indices(finalIndices);
        } else {
            request.indicesOptions(originalOptions);
        }
    }

    private boolean isSystemIndex(Index index, ClusterState state) {
        final IndexMetadata indexMetadata = state.metadata().index(index);
        return indexMetadata == null ? false : IndexMetadata.INDEX_SYSTEM_SETTING.get(indexMetadata.getSettings());
    }

    private boolean isOwnedSystemIndex(Index index) {
        return ownedSystemIndices.stream().anyMatch(descriptor -> descriptor.matchesIndexPattern(index.getName()));
    }
}

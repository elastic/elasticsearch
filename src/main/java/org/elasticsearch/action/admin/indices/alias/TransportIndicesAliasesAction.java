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

package org.elasticsearch.action.admin.indices.alias;

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasAction;
import org.elasticsearch.cluster.metadata.MetaDataIndexAliasesService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.action.admin.indices.alias.delete.AliasesMissingException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;

/**
 * Add/remove aliases action
 */
public class TransportIndicesAliasesAction extends TransportMasterNodeOperationAction<IndicesAliasesRequest, IndicesAliasesResponse> {

    private final MetaDataIndexAliasesService indexAliasesService;

    @Inject
    public TransportIndicesAliasesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, MetaDataIndexAliasesService indexAliasesService, ActionFilters actionFilters) {
        super(settings, IndicesAliasesAction.NAME, transportService, clusterService, threadPool, actionFilters, IndicesAliasesRequest.class);
        this.indexAliasesService = indexAliasesService;
    }

    @Override
    protected String executor() {
        // we go async right away...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesAliasesResponse newResponse() {
        return new IndicesAliasesResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesAliasesRequest request, ClusterState state) {
        Set<String> indices = Sets.newHashSet();
        for (AliasActions aliasAction : request.aliasActions()) {
            for (String index : aliasAction.indices()) {
                indices.add(index);
            }
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indices.toArray(new String[indices.size()]));
    }

    @Override
    protected void masterOperation(final IndicesAliasesRequest request, final ClusterState state, final ActionListener<IndicesAliasesResponse> listener) throws ElasticsearchException {

        //Expand the indices names
        List<AliasActions> actions = request.aliasActions();
        List<AliasAction> finalActions = new ArrayList<>();
        boolean hasOnlyDeletesButNoneCanBeDone = true;
        Set<String> aliases = new HashSet<>();
        for (AliasActions action : actions) {
            //expand indices
            String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), action.indices());
            //collect the aliases
            Collections.addAll(aliases, action.aliases());
            for (String index : concreteIndices) {
                for (String alias : action.concreteAliases(state.metaData(), index)) { 
                    AliasAction finalAction = new AliasAction(action.aliasAction());
                    finalAction.index(index);
                    finalAction.alias(alias);
                    finalActions.add(finalAction);
                    //if there is only delete requests, none will be added if the types do not map to any existing type
                    hasOnlyDeletesButNoneCanBeDone = false;
                }
            }
        }
        if (hasOnlyDeletesButNoneCanBeDone && actions.size() != 0) {
            throw new AliasesMissingException(aliases.toArray(new String[aliases.size()]));
        }
        request.aliasActions().clear();
        IndicesAliasesClusterStateUpdateRequest updateRequest = new IndicesAliasesClusterStateUpdateRequest()
                .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout())
                .actions(finalActions.toArray(new AliasAction[finalActions.size()]));

        indexAliasesService.indicesAliases(updateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new IndicesAliasesResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to perform aliases", t);
                listener.onFailure(t);
            }
        });
    }
}

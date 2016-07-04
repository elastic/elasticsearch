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

package org.elasticsearch.action.admin.indices.get;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;

/**
 * Get index action.
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    @Inject
    public TransportGetIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, GetIndexRequest::new);
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetIndexResponse newResponse() {
        return new GetIndexResponse();
    }

    @Override
    protected void doMasterOperation(final GetIndexRequest request, String[] concreteIndices, final ClusterState state,
                                     final ActionListener<GetIndexResponse> listener) {
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, List<AliasMetaData>> aliasesResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
        Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        for (Feature feature : features) {
            switch (feature) {
            case MAPPINGS:
                    if (!doneMappings) {
                        mappingsResult = state.metaData().findMappings(concreteIndices, request.types());
                        doneMappings = true;
                    }
                    break;
            case ALIASES:
                    if (!doneAliases) {
                        aliasesResult = state.metaData().findAliases(Strings.EMPTY_ARRAY, concreteIndices);
                        doneAliases = true;
                    }
                    break;
            case SETTINGS:
                    if (!doneSettings) {
                        ImmutableOpenMap.Builder<String, Settings> settingsMapBuilder = ImmutableOpenMap.builder();
                        for (String index : concreteIndices) {
                            Settings indexSettings = state.metaData().index(index).getSettings();
                            if (request.humanReadable()) {
                                indexSettings = IndexMetaData.addHumanReadableSettings(indexSettings);
                            }
                            settingsMapBuilder.put(index, indexSettings);
                        }
                        settings = settingsMapBuilder.build();
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new IllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(new GetIndexResponse(concreteIndices, mappingsResult, aliasesResult, settings));
    }
}

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

import com.google.common.collect.ImmutableList;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest.Feature;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.warmer.IndexWarmersMetaData.Entry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Get index action.
 */
public class TransportGetIndexAction extends TransportClusterInfoAction<GetIndexRequest, GetIndexResponse> {

    @Inject
    public TransportGetIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, GetIndexAction.NAME, transportService, clusterService, threadPool, actionFilters, GetIndexRequest.class);
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected GetIndexResponse newResponse() {
        return new GetIndexResponse();
    }

    @Override
    protected void doMasterOperation(final GetIndexRequest request, String[] concreteIndices, final ClusterState state,
                                     final ActionListener<GetIndexResponse> listener) throws ElasticsearchException {
        ImmutableOpenMap<String, ImmutableList<Entry>> warmersResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappingsResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, ImmutableList<AliasMetaData>> aliasesResult = ImmutableOpenMap.of();
        ImmutableOpenMap<String, Settings> settings = ImmutableOpenMap.of();
        Feature[] features = request.features();
        boolean doneAliases = false;
        boolean doneMappings = false;
        boolean doneSettings = false;
        boolean doneWarmers = false;
        for (Feature feature : features) {
            switch (feature) {
            case WARMERS:
                    if (!doneWarmers) {
                        warmersResult = state.metaData().findWarmers(concreteIndices, request.types(), Strings.EMPTY_ARRAY);
                        doneWarmers = true;
                    }
                    break;
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
                            settingsMapBuilder.put(index, state.metaData().index(index).getSettings());
                        }
                        settings = settingsMapBuilder.build();
                        doneSettings = true;
                    }
                    break;

                default:
                    throw new ElasticsearchIllegalStateException("feature [" + feature + "] is not valid");
            }
        }
        listener.onResponse(new GetIndexResponse(concreteIndices, warmersResult, mappingsResult, aliasesResult, settings));
    }
}

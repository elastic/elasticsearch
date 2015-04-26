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

package org.elasticsearch.action.admin.indices.settings.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 */
public class TransportGetSettingsAction extends TransportMasterNodeReadOperationAction<GetSettingsRequest, GetSettingsResponse> {

    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, SettingsFilter settingsFilter, ActionFilters actionFilters) {
        super(settings, GetSettingsAction.NAME, transportService, clusterService, threadPool, actionFilters, GetSettingsRequest.class);
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected String executor() {
        // Very lightweight operation
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetSettingsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }


    @Override
    protected GetSettingsResponse newResponse() {
        return new GetSettingsResponse();
    }

    @Override
    protected void masterOperation(GetSettingsRequest request, ClusterState state, ActionListener<GetSettingsResponse> listener) throws ElasticsearchException {
        String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        ImmutableOpenMap.Builder<String, Settings> indexToSettingsBuilder = ImmutableOpenMap.builder();
        for (String concreteIndex : concreteIndices) {
            IndexMetaData indexMetaData = state.getMetaData().index(concreteIndex);
            if (indexMetaData == null) {
                continue;
            }

            Settings settings = SettingsFilter.filterSettings(settingsFilter.getPatterns(), indexMetaData.settings());
            if (!CollectionUtils.isEmpty(request.names())) {
                ImmutableSettings.Builder settingsBuilder = ImmutableSettings.builder();
                for (Map.Entry<String, String> entry : settings.getAsMap().entrySet()) {
                    if (Regex.simpleMatch(request.names(), entry.getKey())) {
                        settingsBuilder.put(entry.getKey(), entry.getValue());
                    }
                }
                settings = settingsBuilder.build();
            }
            indexToSettingsBuilder.put(concreteIndex, settings);
        }
        listener.onResponse(new GetSettingsResponse(indexToSettingsBuilder.build()));
    }
}

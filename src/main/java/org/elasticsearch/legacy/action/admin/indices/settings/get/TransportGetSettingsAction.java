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

package org.elasticsearch.legacy.action.admin.indices.settings.get;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ActionListener;
import org.elasticsearch.legacy.action.support.master.TransportMasterNodeReadOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.metadata.IndexMetaData;
import org.elasticsearch.legacy.common.collect.ImmutableOpenMap;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.regex.Regex;
import org.elasticsearch.legacy.common.settings.ImmutableSettings;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.common.settings.SettingsFilter;
import org.elasticsearch.legacy.common.util.CollectionUtils;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.Map;

/**
 */
public class TransportGetSettingsAction extends TransportMasterNodeReadOperationAction<GetSettingsRequest, GetSettingsResponse> {

    private final SettingsFilter settingsFilter;

    @Inject
    public TransportGetSettingsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, SettingsFilter settingsFilter) {
        super(settings, GetSettingsAction.NAME, transportService, clusterService, threadPool);
        this.settingsFilter = settingsFilter;
    }

    @Override
    protected String executor() {
        // Very lightweight operation
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetSettingsRequest newRequest() {
        return new GetSettingsRequest();
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

            Settings settings = settingsFilter.filterSettings(indexMetaData.settings());
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

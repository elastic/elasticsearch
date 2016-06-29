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

package org.elasticsearch.percolator;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PercolatorPlugin extends Plugin implements MapperPlugin, ActionPlugin {

    public static final String NAME = "percolator";

    private final boolean transportClientMode;
    private final Settings settings;

    public PercolatorPlugin(Settings settings) {
        this.transportClientMode = transportClientMode(settings);
        this.settings = settings;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest<?>, ? extends ActionResponse>> getActions() {
        return Arrays.asList(new ActionHandler<>(PercolateAction.INSTANCE, TransportPercolateAction.class),
                new ActionHandler<>(MultiPercolateAction.INSTANCE, TransportMultiPercolateAction.class));
    }

    public void onModule(NetworkModule module) {
        if (transportClientMode == false) {
            module.registerRestHandler(RestPercolateAction.class);
            module.registerRestHandler(RestMultiPercolateAction.class);
        }
    }

    public void onModule(SearchModule module) {
        module.registerQuery(PercolateQueryBuilder::new, PercolateQueryBuilder::fromXContent, PercolateQueryBuilder.QUERY_NAME_FIELD);
        module.registerFetchSubPhase(new PercolatorHighlightSubFetchPhase(settings, module.getHighlighters()));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(PercolatorFieldMapper.INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING);
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(PercolatorFieldMapper.CONTENT_TYPE, new PercolatorFieldMapper.TypeParser());
    }

    static boolean transportClientMode(Settings settings) {
        return TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
    }
}

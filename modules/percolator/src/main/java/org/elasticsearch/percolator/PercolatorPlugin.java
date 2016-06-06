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

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchModule;

public class PercolatorPlugin extends Plugin {

    public static final String NAME = "percolator";

    private final boolean transportClientMode;

    public PercolatorPlugin(Settings settings) {
        this.transportClientMode = transportClientMode(settings);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Percolator module adds capability to index queries and query these queries by specifying documents";
    }

    public void onModule(ActionModule module) {
        module.registerAction(PercolateAction.INSTANCE, TransportPercolateAction.class);
        module.registerAction(MultiPercolateAction.INSTANCE, TransportMultiPercolateAction.class);
    }

    public void onModule(NetworkModule module) {
        if (transportClientMode == false) {
            module.registerRestHandler(RestPercolateAction.class);
            module.registerRestHandler(RestMultiPercolateAction.class);
        }
    }

    public void onModule(IndicesModule module) {
        module.registerMapper(PercolatorFieldMapper.CONTENT_TYPE, new PercolatorFieldMapper.TypeParser());
    }

    public void onModule(SearchModule module) {
        module.registerQuery(PercolateQueryBuilder::new, PercolateQueryBuilder::fromXContent, PercolateQueryBuilder.QUERY_NAME_FIELD);
        module.registerFetchSubPhase(PercolatorHighlightSubFetchPhase.class);
    }

    public void onModule(SettingsModule module) {
        module.registerSetting(PercolatorFieldMapper.INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING);
    }

    static boolean transportClientMode(Settings settings) {
        return TransportClient.CLIENT_TYPE.equals(settings.get(Client.CLIENT_TYPE_SETTING_S.getKey()));
    }
}

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
package org.elasticsearch.messy.tests;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ScriptIndexSettingsTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(GroovyPlugin.class);
    }

    @Override
    public void randomIndexTemplate() throws IOException {
        // don't set random index template, because we are testing here what happens if no custom settings have been
        // specified
    }

    @Override
    public Settings indexSettings() {
        // don't set random index settings, because we are testing here what happens if no custom settings have been
        // specified
        return Settings.EMPTY;
    }

    @Test
    public void testScriptIndexSettings() {
        PutIndexedScriptResponse putIndexedScriptResponse =
                client().preparePutIndexedScript().setId("foobar").setScriptLang("groovy").setSource("{ \"script\": 1 }")
                        .get();
        assertTrue(putIndexedScriptResponse.isCreated());
        ensureGreen();

        IndicesExistsRequest existsRequest = new IndicesExistsRequest();
        String[] index = new String[1];
        index[0] = ScriptService.SCRIPT_INDEX;
        existsRequest.indices(index);


        IndicesExistsResponse existsResponse = cluster().client().admin().indices().exists(existsRequest).actionGet();
        assertTrue(existsResponse.isExists());

        GetSettingsRequest settingsRequest = new GetSettingsRequest();
        settingsRequest.indices(ScriptService.SCRIPT_INDEX);
        settingsRequest.indicesOptions(IndicesOptions.strictExpandOpen());
        GetSettingsResponse settingsResponse = client()
                .admin()
                .indices()
                .getSettings(settingsRequest)
                .actionGet();

        String numberOfShards = settingsResponse.getSetting(ScriptService.SCRIPT_INDEX,"index.number_of_shards");
        String numberOfReplicas = settingsResponse.getSetting(ScriptService.SCRIPT_INDEX,"index.auto_expand_replicas");

        assertEquals("Number of shards should be 1", "1", numberOfShards);
        assertEquals("Auto expand replicas should be 0-all", "0-all", numberOfReplicas);
    }

    @Test
    public void testScriptIndexDefaults() {
        createIndex(ScriptService.SCRIPT_INDEX);
        IndexMetaData indexMetaData = client().admin().cluster().prepareState().get()
                .getState().getMetaData().index(ScriptService.SCRIPT_INDEX);
        assertThat(indexMetaData.getNumberOfShards(), equalTo(1));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(0));
        assertThat(indexMetaData.getSettings().get("index.auto_expand_replicas"), equalTo("0-all"));

        client().admin().indices().prepareDelete(ScriptService.SCRIPT_INDEX).get();
        client().admin().indices().prepareCreate(ScriptService.SCRIPT_INDEX)
                .setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 3)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2))
                .get();
        indexMetaData = client().admin().cluster().prepareState().get()
                .getState().getMetaData().index(ScriptService.SCRIPT_INDEX);
        assertThat(indexMetaData.getNumberOfShards(), equalTo(3));
        assertThat(indexMetaData.getNumberOfReplicas(), equalTo(2));
        assertThat(indexMetaData.getSettings().get("index.auto_expand_replicas"), nullValue());
    }

    @Test
    public void testDeleteScriptIndex() {
        PutIndexedScriptResponse putIndexedScriptResponse =
                client().preparePutIndexedScript().setId("foobar").setScriptLang("groovy").setSource("{ \"script\": 1 }")
                        .get();
        assertTrue(putIndexedScriptResponse.isCreated());
        DeleteIndexResponse deleteResponse = client().admin().indices().prepareDelete(ScriptService.SCRIPT_INDEX).get();
        assertTrue(deleteResponse.isAcknowledged());
        ensureGreen();
        try {
            GetIndexedScriptResponse response = client().prepareGetIndexedScript("groovy","foobar").get();
            assertTrue(false); //This should not happen
        } catch (IndexNotFoundException ime) {
            assertTrue(true);
        }
    }



}

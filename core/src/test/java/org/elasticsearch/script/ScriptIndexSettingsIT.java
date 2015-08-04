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
package org.elasticsearch.script;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class ScriptIndexSettingsIT extends ESIntegTestCase {


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

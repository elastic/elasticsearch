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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.bootstrap.Elasticsearch;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

//Use test scope so that tmp paths get set correctly
//Use a single node because we write a file
//This is ok since we are just testing to see if written files are picked up correctly
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, maxNumDataNodes = 1, minNumDataNodes = 1, numDataNodes = 1, numClientNodes = 0, enableRandomBenchNodes = false, transportClientRatio = 1)
public class OnDiskScriptLoadingTests extends ElasticsearchIntegrationTest {

    private Path scriptLocation;
    private Path confLocation;
    private TimeValue maxDelayBeforeFilesAvailable;
    private boolean isAutoReloadedEnabled;

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        //Set path so ScriptService will pick up the test scripts
        File newTempDir = newTempDir(LifecycleScope.TEST);

        confLocation = Paths.get(newTempDir.getPath());
        scriptLocation = Paths.get(confLocation.toString(), "scripts");
        try {
            Files.createDirectory(scriptLocation);
        } catch (IOException io) {
            logger.error("Failed to create tmp script location [" + scriptLocation + "]", io);
        }


        maxDelayBeforeFilesAvailable = super.nodeSettings(nodeOrdinal).
                getAsTime(ScriptService.SCRIPT_RELOAD_INTERVAL_SETTING, ScriptService.DEFAULT_AUTO_RELOAD_INTERVAL);
        isAutoReloadedEnabled = super.nodeSettings(nodeOrdinal).getAsBoolean("script."+ScriptService.AUTO_RELOAD_ENABLED_COMPONENT_SETTING, ScriptService.DEFAULT_AUTO_RELOAD_SETTING);

        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", confLocation).build();
    }

    @Test
    public void testNewFilesArePickedUp() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = new ArrayList();
        builders.add(client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "2").setSource("{\"theField\":\"foo 2\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "3").setSource("{\"theField\":\"foo 3\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "4").setSource("{\"theField\":\"foo 4\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "5").setSource("{\"theField\":\"bar\"}"));
        indexRandom(true, builders);

        Path f = Paths.get(scriptLocation.toString(), "newscript.groovy");
        try (BufferedWriter writer = Files.newBufferedWriter(f, UTF8)) {
            writer.write("4");
            writer.flush();
        } catch (IOException ie) {
            throw new ElasticsearchException("Failed to open writer to " + f.toString(), ie);
        }
        logger.error("WROTE FILE [{}]", f.toAbsolutePath());
        //Wait for new file to be picked up
        sleep(maxDelayBeforeFilesAvailable.getMillis()+1000); //Add an extra second just incase it takes a little longer
        //sleep(10000);

        String query = "{ \"query\" : { \"match_all\": {}} , \"script_fields\" : { \"test1\" : { \"file\" : \"newscript\", \"script_lang\": \"groovy\" }}, \"size\":1}";
        logger.info("Running : [{}]", query);
        try {
            SearchResponse searchResponse = client().prepareSearch().setSource(query).setIndices("test").setTypes("scriptTest").get();
            assertTrue(isAutoReloadedEnabled);
            assertHitCount(searchResponse, 5);
            assertThat(searchResponse.getHits().hits().length, equalTo(1));
            SearchHit sh = searchResponse.getHits().getAt(0);
            assertThat((Integer) sh.field("test1").getValue(), equalTo(4));
        } catch (ElasticsearchException ee) {
            logger.error("Failed to run search with new script isAutoReloadEnabled : [{}]", ee, isAutoReloadedEnabled);
            assertFalse(isAutoReloadedEnabled); //This is an expected fail if we are not reloading scripts
        }
    }


    @Test
    public void testAutoReloadTimeIsDynamicallyConfigurable() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = new ArrayList();
        builders.add(client().prepareIndex("test", "scriptTest", "1").setSource("{\"theField\":\"foo\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "2").setSource("{\"theField\":\"foo 2\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "3").setSource("{\"theField\":\"foo 3\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "4").setSource("{\"theField\":\"foo 4\"}"));
        builders.add(client().prepareIndex("test", "scriptTest", "5").setSource("{\"theField\":\"bar\"}"));
        indexRandom(true,builders);

        Map<String,Object> settingsMap = new HashMap<>();
        settingsMap.put(ScriptService.SCRIPT_RELOAD_INTERVAL_SETTING, new TimeValue(maxDelayBeforeFilesAvailable.getMillis()/2) );
        ClusterUpdateSettingsRequest settingsRequest =
                (new ClusterUpdateSettingsRequestBuilder(client().admin().cluster())).setTransientSettings(settingsMap)
                        .request();
        ClusterUpdateSettingsResponse settingsResponse = client().admin().cluster().updateSettings(settingsRequest).actionGet();
        assertAcked(settingsResponse);

        Path f = Paths.get(scriptLocation.toString(),"newscript.groovy");

        try (BufferedWriter writer = Files.newBufferedWriter(f, UTF8)) {
            writer.write("4");
            writer.flush();
        } catch (IOException ie) {
            throw new ElasticsearchException("Failed to open writer to " + f.toString(), ie);
        }

        //Wait for new file to be picked up
        sleep((maxDelayBeforeFilesAvailable.getMillis()/2)+1000); //Add an extra second just incase it takes a little longer
        String query = "{ \"query\" : { \"match_all\": {}} , \"script_fields\" : { \"test1\" : { \"file\" : \"newscript\" }}, \"size\":1}";
        logger.info("Running : [{}]", query);
        try {
            SearchResponse searchResponse = client().prepareSearch().setSource(query).setIndices("test").setTypes("scriptTest").get();
            assertTrue(isAutoReloadedEnabled);
            assertHitCount(searchResponse, 5);
            assertThat(searchResponse.getHits().hits().length, equalTo(1));
            SearchHit sh = searchResponse.getHits().getAt(0);
            assertThat((Integer) sh.field("test1").getValue(), equalTo(4));
        } catch (ElasticsearchException ee) {
            logger.error("Failed to run search with new script isAutoReloadEnabled : [{}]", ee, isAutoReloadedEnabled);
            assertFalse(isAutoReloadedEnabled); //This is an expected fail if we are not reloading scripts
        }
    }

}

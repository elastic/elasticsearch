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
package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Constants;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

/**
 */
public class IndexStoreBWCTests extends ESSingleNodeTestCase {


    public void testOldCoreTypesFail() {
        try {
            createIndex("test", Settings.builder().put(IndexStoreModule.STORE_TYPE, "nio_fs").build());
            fail();
        } catch (Exception ex) {
        }
        try {
            createIndex("test", Settings.builder().put(IndexStoreModule.STORE_TYPE, "mmap_fs").build());
            fail();
        } catch (Exception ex) {
        }
        try {
            createIndex("test", Settings.builder().put(IndexStoreModule.STORE_TYPE, "simple_fs").build());
            fail();
        } catch (Exception ex) {
        }
    }

    public void testUpgradeCoreTypes() throws IOException {
        String type = RandomPicks.randomFrom(random(), Arrays.asList("nio", "mmap", "simple"));
        createIndex("test", Settings.builder()
                .put(IndexStoreModule.STORE_TYPE, type+"fs")
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.V_1_7_0)
                .build());

        client().admin().indices().prepareClose("test").get();
        client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder()
                .put(IndexStoreModule.STORE_TYPE, type + "_fs").build()).get();
        GetSettingsResponse getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        String actualType = getSettingsResponse.getSetting("test", IndexStoreModule.STORE_TYPE);
        assertEquals(type + "_fs", actualType);

        // now reopen and upgrade
        client().admin().indices().prepareOpen("test").get();

        getSettingsResponse = client().admin().indices().prepareGetSettings("test").get();
        actualType = getSettingsResponse.getSetting("test", IndexStoreModule.STORE_TYPE);
        assertEquals(type+"fs", actualType);
    }

}

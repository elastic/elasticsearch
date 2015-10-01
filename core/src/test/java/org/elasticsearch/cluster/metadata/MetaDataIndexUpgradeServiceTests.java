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
package org.elasticsearch.cluster.metadata;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.IndexStoreModule;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Locale;

public class MetaDataIndexUpgradeServiceTests extends ESTestCase {

    public void testUpgradeStoreSettings() {
        final String type = RandomPicks.randomFrom(random(), Arrays.asList("nio_fs", "mmap_fs", "simple_fs", "default", "fs"));
        MetaDataIndexUpgradeService metaDataIndexUpgradeService = new MetaDataIndexUpgradeService(Settings.EMPTY, null);
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexStoreModule.STORE_TYPE, randomBoolean() ? type : type.toUpperCase(Locale.ROOT))
                .build();
        IndexMetaData test = IndexMetaData.builder("test")
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        IndexMetaData indexMetaData = metaDataIndexUpgradeService.upgradeSettings(test);
        assertEquals(type.replace("_", ""), indexMetaData.getSettings().get(IndexStoreModule.STORE_TYPE));
    }

    public void testNoStoreSetting() {
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        IndexMetaData test = IndexMetaData.builder("test")
                .settings(indexSettings)
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        MetaDataIndexUpgradeService metaDataIndexUpgradeService = new MetaDataIndexUpgradeService(Settings.EMPTY, null);
        IndexMetaData indexMetaData = metaDataIndexUpgradeService.upgradeSettings(test);
        assertSame(indexMetaData, test);
    }
}

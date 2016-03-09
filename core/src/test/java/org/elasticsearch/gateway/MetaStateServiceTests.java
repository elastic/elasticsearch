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
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.PersistedIndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class MetaStateServiceTests extends ESTestCase {
    private static Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    public void testWriteLoadIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);

            final IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            final String clusterUUID = Strings.randomBase64UUID();
            final PersistedIndexMetaData persistedIndex = new PersistedIndexMetaData(index, clusterUUID);
            metaStateService.writeIndex("test_write", persistedIndex);
            final PersistedIndexMetaData readPersistedIndex = metaStateService.loadIndexState(index.getIndex());
            assertThat(readPersistedIndex.getIndexMetaData(), equalTo(index));
            assertThat(readPersistedIndex.getClusterUUID(), equalTo(clusterUUID));
        }
    }

    public void testLoadMissingIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);
            assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
        }
    }

    public void testWriteLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            metaStateService.writeGlobalState("test_write", metaData);
            assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
        }
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaDataWithIndex = MetaData.builder(metaData).put(index, true).build();

            metaStateService.writeGlobalState("test_write", metaDataWithIndex);
            assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(metaStateService.loadGlobalState().hasIndex("test1"), equalTo(false));
        }
    }

    public void testLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env);

            final String clusterUUID = Strings.randomBase64UUID();
            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            final PersistedIndexMetaData persistedIndex = new PersistedIndexMetaData(index, clusterUUID);
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .put(index, true)
                    .clusterUUID(clusterUUID)
                    .build();

            metaStateService.writeGlobalState("test_write", metaData);
            metaStateService.writeIndex("test_write", persistedIndex);

            MetaData loadedState = metaStateService.loadFullState();
            assertThat(loadedState.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedState.hasIndex("test1"), equalTo(true));
            assertThat(loadedState.index("test1"), equalTo(index));
            assertThat(loadedState.clusterUUID(), equalTo(clusterUUID));
        }
    }
}

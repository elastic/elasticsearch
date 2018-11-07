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
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class MetaStateServiceTests extends ESTestCase {
    private static Settings indexSettings = Settings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    public void testWriteLoadIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            metaStateService.writeIndex("test_write", index);
            assertThat(metaStateService.loadIndexState(index.getIndex()), equalTo(index));
        }
    }

    public void testLoadMissingIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
        }
    }

    public void testWriteLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            metaStateService.writeGlobalState("test_write", metaData);
            assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
        }
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

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

    public void testLoadFullStateBWC() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            IndexMetaData indexMetaData = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .put(indexMetaData, true)
                    .build();

            long globalGeneration = metaStateService.writeGlobalState("test_write", metaData);
            long indexGeneration = metaStateService.writeIndex("test_write", indexMetaData);

            Tuple<Manifest, MetaData> stateAndData = metaStateService.loadFullState();
            Manifest manifest = stateAndData.v1();
            assertThat(manifest.getGlobalStateGeneration(), equalTo(globalGeneration));
            assertThat(manifest.getIndices(), hasKey(indexMetaData.getIndex()));
            assertThat(manifest.getIndices().get(indexMetaData.getIndex()), equalTo(indexGeneration));

            MetaData loadedMetaData = stateAndData.v2();
            assertThat(loadedMetaData.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
            assertThat(loadedMetaData.index("test1"), equalTo(indexMetaData));
        }
    }

    public void testLoadEmptyState() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            Tuple<Manifest, MetaData> stateAndData = metaStateService.loadFullState();

            Manifest manifest = stateAndData.v1();
            assertThat(manifest.getGlobalStateGeneration(), equalTo(-1L));
            assertThat(manifest.getIndices().entrySet(), empty());

            MetaData metaData = stateAndData.v2();
            MetaData emptyMetaData = MetaData.builder().build();
            assertTrue(MetaData.isGlobalStateEquals(metaData, emptyMetaData));
        }
    }

    public void testLoadFullStateAndUpdate() throws IOException {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .put(index, true)
                    .build();

            long globalGeneration = metaStateService.writeGlobalState("first global state write", metaData);
            long indexGeneration = metaStateService.writeIndex("first index state write", index);

            Manifest manifest = new Manifest(globalGeneration, new HashMap<Index, Long>() {{
                put(index.getIndex(), indexGeneration);
            }});

            metaStateService.writeManifest("first meta state write", manifest);

            MetaData newMetaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value2").build())
                    .put(index, true)
                    .build();
            globalGeneration = metaStateService.writeGlobalState("second global state write", newMetaData);

            Tuple<Manifest, MetaData> stateAndData = metaStateService.loadFullState();
            assertThat(stateAndData.v1(), equalTo(manifest));

            MetaData loadedMetaData = stateAndData.v2();
            assertThat(loadedMetaData.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
            assertThat(loadedMetaData.index("test1"), equalTo(index));

            manifest = new Manifest(globalGeneration, new HashMap<Index, Long>() {{
                put(index.getIndex(), indexGeneration);
            }});

            long metaStateGeneration = metaStateService.writeManifest("test", manifest);
            metaStateService.cleanupGlobalState(globalGeneration);
            metaStateService.cleanupIndex(index.getIndex(), indexGeneration);
            metaStateService.cleanupMetaState(metaStateGeneration);

            stateAndData = metaStateService.loadFullState();
            assertThat(stateAndData.v1(), equalTo(manifest));

            loadedMetaData = stateAndData.v2();
            assertThat(loadedMetaData.persistentSettings(), equalTo(newMetaData.persistentSettings()));
            assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
            assertThat(loadedMetaData.index("test1"), equalTo(index));
        }
    }
}

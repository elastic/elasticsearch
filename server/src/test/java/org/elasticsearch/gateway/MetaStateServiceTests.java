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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class MetaStateServiceTests extends ESTestCase {

    private NodeEnvironment env;
    private MetaStateService metaStateService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        env = newNodeEnvironment();
        metaStateService = new MetaStateService(env, xContentRegistry());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        env.close();
    }

    private static IndexMetaData indexMetaData(String name) {
        return IndexMetaData.builder(name).settings(
                Settings.builder()
                        .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
        ).build();
    }

    public void testWriteLoadIndex() throws Exception {
        IndexMetaData index = indexMetaData("test1");
        metaStateService.writeIndex("test_write", index);
        assertThat(metaStateService.loadIndexState(index.getIndex()), equalTo(index));
    }

    public void testLoadMissingIndex() throws Exception {
        assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
    }

    public void testWriteLoadGlobal() throws Exception {
        MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .build();
        metaStateService.writeGlobalState("test_write", metaData);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .build();
        IndexMetaData index = indexMetaData("test1");
        MetaData metaDataWithIndex = MetaData.builder(metaData).put(index, true).build();

        metaStateService.writeGlobalState("test_write", metaDataWithIndex);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
        assertThat(metaStateService.loadGlobalState().hasIndex("test1"), equalTo(false));
    }

    public void testLoadFullStateBWC() throws Exception {
        IndexMetaData indexMetaData = indexMetaData("test1");
        MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .put(indexMetaData, true)
                .build();

        long globalGeneration = metaStateService.writeGlobalState("test_write", metaData);
        long indexGeneration = metaStateService.writeIndex("test_write", indexMetaData);

        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        Manifest manifest = manifestAndMetaData.v1();
        assertThat(manifest.getGlobalGeneration(), equalTo(globalGeneration));
        assertThat(manifest.getIndexGenerations(), hasKey(indexMetaData.getIndex()));
        assertThat(manifest.getIndexGenerations().get(indexMetaData.getIndex()), equalTo(indexGeneration));

        MetaData loadedMetaData = manifestAndMetaData.v2();
        assertThat(loadedMetaData.persistentSettings(), equalTo(metaData.persistentSettings()));
        assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetaData.index("test1"), equalTo(indexMetaData));
    }

    public void testLoadEmptyStateNoManifest() throws IOException {
        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();

        Manifest manifest = manifestAndMetaData.v1();
        assertTrue(manifest.isEmpty());

        MetaData metaData = manifestAndMetaData.v2();
        assertTrue(MetaData.isGlobalStateEquals(metaData, MetaData.EMPTY_META_DATA));
    }

    public void testLoadEmptyStateWithManifest() throws IOException {
        Manifest manifest = Manifest.empty();
        metaStateService.writeManifestAndCleanup("test", manifest);

        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        assertTrue(manifestAndMetaData.v1().isEmpty());
        MetaData metaData = manifestAndMetaData.v2();
        assertTrue(MetaData.isGlobalStateEquals(metaData, MetaData.EMPTY_META_DATA));
    }

    public void testLoadFullStateMissingGlobalMetaData() throws IOException {
        IndexMetaData index = indexMetaData("test1");
        long indexGeneration = metaStateService.writeIndex("test", index);
        Manifest manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                Manifest.empty().getGlobalGeneration(), new HashMap<Index, Long>() {{
                    put(index.getIndex(), indexGeneration);
                }});
        assertTrue(manifest.isGlobalGenerationMissing());
        metaStateService.writeManifestAndCleanup("test", manifest);

        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        assertThat(manifestAndMetaData.v1(), equalTo(manifest));
        MetaData loadedMetaData = manifestAndMetaData.v2();
        assertTrue(MetaData.isGlobalStateEquals(loadedMetaData, MetaData.EMPTY_META_DATA));
        assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetaData.index("test1"), equalTo(index));
    }

    public void testLoadFullStateAndUpdateAndClean() throws IOException {
        IndexMetaData index = indexMetaData("test1");
        MetaData metaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .put(index, true)
                .build();

        long globalGeneration = metaStateService.writeGlobalState("first global state write", metaData);
        long indexGeneration = metaStateService.writeIndex("first index state write", index);

        Manifest manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                globalGeneration, new HashMap<Index, Long>() {{
            put(index.getIndex(), indexGeneration);
        }});
        metaStateService.writeManifestAndCleanup("first manifest write", manifest);

        MetaData newMetaData = MetaData.builder()
                .persistentSettings(Settings.builder().put("test1", "value2").build())
                .put(index, true)
                .build();
        globalGeneration = metaStateService.writeGlobalState("second global state write", newMetaData);

        Tuple<Manifest, MetaData> manifestAndMetaData = metaStateService.loadFullState();
        assertThat(manifestAndMetaData.v1(), equalTo(manifest));

        MetaData loadedMetaData = manifestAndMetaData.v2();
        assertThat(loadedMetaData.persistentSettings(), equalTo(metaData.persistentSettings()));
        assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetaData.index("test1"), equalTo(index));

        manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                globalGeneration, new HashMap<Index, Long>() {{
            put(index.getIndex(), indexGeneration);
        }});

        metaStateService.writeManifestAndCleanup("second manifest write", manifest);
        metaStateService.cleanupGlobalState(globalGeneration);
        metaStateService.cleanupIndex(index.getIndex(), indexGeneration);

        manifestAndMetaData = metaStateService.loadFullState();
        assertThat(manifestAndMetaData.v1(), equalTo(manifest));

        loadedMetaData = manifestAndMetaData.v2();
        assertThat(loadedMetaData.persistentSettings(), equalTo(newMetaData.persistentSettings()));
        assertThat(loadedMetaData.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetaData.index("test1"), equalTo(index));

        if (randomBoolean()) {
            metaStateService.unreferenceAll();
        } else {
            metaStateService.deleteAll();
        }
        manifestAndMetaData = metaStateService.loadFullState();
        assertTrue(manifestAndMetaData.v1().isEmpty());
        metaData = manifestAndMetaData.v2();
        assertTrue(MetaData.isGlobalStateEquals(metaData, MetaData.EMPTY_META_DATA));
    }
}

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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
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

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name).settings(
                Settings.builder()
                        .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .build()
        ).build();
    }

    public void testWriteLoadIndex() throws Exception {
        IndexMetadata index = indexMetadata("test1");
        metaStateService.writeIndex("test_write", index);
        assertThat(metaStateService.loadIndexState(index.getIndex()), equalTo(index));
    }

    public void testLoadMissingIndex() throws Exception {
        assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
    }

    public void testWriteLoadGlobal() throws Exception {
        Metadata metadata = Metadata.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .build();
        metaStateService.writeGlobalState("test_write", metadata);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metadata.persistentSettings()));
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        Metadata metadata = Metadata.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .build();
        IndexMetadata index = indexMetadata("test1");
        Metadata metadataWithIndex = Metadata.builder(metadata).put(index, true).build();

        metaStateService.writeGlobalState("test_write", metadataWithIndex);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metadata.persistentSettings()));
        assertThat(metaStateService.loadGlobalState().hasIndex("test1"), equalTo(false));
    }

    public void testLoadFullStateBWC() throws Exception {
        IndexMetadata indexMetadata = indexMetadata("test1");
        Metadata metadata = Metadata.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .put(indexMetadata, true)
                .build();

        long globalGeneration = metaStateService.writeGlobalState("test_write", metadata);
        long indexGeneration = metaStateService.writeIndex("test_write", indexMetadata);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        Manifest manifest = manifestAndMetadata.v1();
        assertThat(manifest.getGlobalGeneration(), equalTo(globalGeneration));
        assertThat(manifest.getIndexGenerations(), hasKey(indexMetadata.getIndex()));
        assertThat(manifest.getIndexGenerations().get(indexMetadata.getIndex()), equalTo(indexGeneration));

        Metadata loadedMetadata = manifestAndMetadata.v2();
        assertThat(loadedMetadata.persistentSettings(), equalTo(metadata.persistentSettings()));
        assertThat(loadedMetadata.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetadata.index("test1"), equalTo(indexMetadata));
    }

    public void testLoadEmptyStateNoManifest() throws IOException {
        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();

        Manifest manifest = manifestAndMetadata.v1();
        assertTrue(manifest.isEmpty());

        Metadata metadata = manifestAndMetadata.v2();
        assertTrue(Metadata.isGlobalStateEquals(metadata, Metadata.EMPTY_METADATA));
    }

    public void testLoadEmptyStateWithManifest() throws IOException {
        Manifest manifest = Manifest.empty();
        metaStateService.writeManifestAndCleanup("test", manifest);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        assertTrue(manifestAndMetadata.v1().isEmpty());
        Metadata metadata = manifestAndMetadata.v2();
        assertTrue(Metadata.isGlobalStateEquals(metadata, Metadata.EMPTY_METADATA));
    }

    public void testLoadFullStateMissingGlobalMetadata() throws IOException {
        IndexMetadata index = indexMetadata("test1");
        long indexGeneration = metaStateService.writeIndex("test", index);
        Manifest manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                Manifest.empty().getGlobalGeneration(), new HashMap<Index, Long>() {{
                    put(index.getIndex(), indexGeneration);
                }});
        assertTrue(manifest.isGlobalGenerationMissing());
        metaStateService.writeManifestAndCleanup("test", manifest);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        assertThat(manifestAndMetadata.v1(), equalTo(manifest));
        Metadata loadedMetadata = manifestAndMetadata.v2();
        assertTrue(Metadata.isGlobalStateEquals(loadedMetadata, Metadata.EMPTY_METADATA));
        assertThat(loadedMetadata.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetadata.index("test1"), equalTo(index));
    }

    public void testLoadFullStateAndUpdateAndClean() throws IOException {
        IndexMetadata index = indexMetadata("test1");
        Metadata metadata = Metadata.builder()
                .persistentSettings(Settings.builder().put("test1", "value1").build())
                .put(index, true)
                .build();

        long globalGeneration = metaStateService.writeGlobalState("first global state write", metadata);
        long indexGeneration = metaStateService.writeIndex("first index state write", index);

        Manifest manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                globalGeneration, new HashMap<Index, Long>() {{
            put(index.getIndex(), indexGeneration);
        }});
        metaStateService.writeManifestAndCleanup("first manifest write", manifest);

        Metadata newMetadata = Metadata.builder()
                .persistentSettings(Settings.builder().put("test1", "value2").build())
                .put(index, true)
                .build();
        globalGeneration = metaStateService.writeGlobalState("second global state write", newMetadata);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        assertThat(manifestAndMetadata.v1(), equalTo(manifest));

        Metadata loadedMetadata = manifestAndMetadata.v2();
        assertThat(loadedMetadata.persistentSettings(), equalTo(metadata.persistentSettings()));
        assertThat(loadedMetadata.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetadata.index("test1"), equalTo(index));

        manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(),
                globalGeneration, new HashMap<Index, Long>() {{
            put(index.getIndex(), indexGeneration);
        }});

        metaStateService.writeManifestAndCleanup("second manifest write", manifest);
        metaStateService.cleanupGlobalState(globalGeneration);
        metaStateService.cleanupIndex(index.getIndex(), indexGeneration);

        manifestAndMetadata = metaStateService.loadFullState();
        assertThat(manifestAndMetadata.v1(), equalTo(manifest));

        loadedMetadata = manifestAndMetadata.v2();
        assertThat(loadedMetadata.persistentSettings(), equalTo(newMetadata.persistentSettings()));
        assertThat(loadedMetadata.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetadata.index("test1"), equalTo(index));

        if (randomBoolean()) {
            metaStateService.unreferenceAll();
        } else {
            metaStateService.deleteAll();
        }
        manifestAndMetadata = metaStateService.loadFullState();
        assertTrue(manifestAndMetadata.v1().isEmpty());
        metadata = manifestAndMetadata.v2();
        assertTrue(Metadata.isGlobalStateEquals(metadata, Metadata.EMPTY_METADATA));
    }
}

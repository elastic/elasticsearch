/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
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
        return IndexMetadata.builder(name)
            .settings(indexSettings(Version.CURRENT, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID()))
            .build();
    }

    public void testWriteLoadIndex() throws Exception {
        IndexMetadata index = indexMetadata("test1");
        MetaStateWriterUtils.writeIndex(env, "test_write", index);
        assertThat(metaStateService.loadIndexState(index.getIndex()), equalTo(index));
    }

    public void testLoadMissingIndex() throws Exception {
        assertThat(metaStateService.loadIndexState(new Index("test1", "test1UUID")), nullValue());
    }

    public void testWriteLoadGlobal() throws Exception {
        Metadata metadata = Metadata.builder().persistentSettings(Settings.builder().put("test1", "value1").build()).build();
        MetaStateWriterUtils.writeGlobalState(env, "test_write", metadata);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metadata.persistentSettings()));
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        Metadata metadata = Metadata.builder().persistentSettings(Settings.builder().put("test1", "value1").build()).build();
        IndexMetadata index = indexMetadata("test1");
        Metadata metadataWithIndex = Metadata.builder(metadata).put(index, true).build();

        MetaStateWriterUtils.writeGlobalState(env, "test_write", metadataWithIndex);
        assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metadata.persistentSettings()));
        assertThat(metaStateService.loadGlobalState().hasIndex("test1"), equalTo(false));
    }

    public void testLoadFullStateBWC() throws Exception {
        IndexMetadata indexMetadata = indexMetadata("test1");
        Metadata metadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test1", "value1").build())
            .put(indexMetadata, true)
            .build();

        long globalGeneration = MetaStateWriterUtils.writeGlobalState(env, "test_write", metadata);
        long indexGeneration = MetaStateWriterUtils.writeIndex(env, "test_write", indexMetadata);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        Manifest manifest = manifestAndMetadata.v1();
        assertThat(manifest.globalGeneration(), equalTo(globalGeneration));
        assertThat(manifest.indexGenerations(), hasKey(indexMetadata.getIndex()));
        assertThat(manifest.indexGenerations().get(indexMetadata.getIndex()), equalTo(indexGeneration));

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
        MetaStateWriterUtils.writeManifestAndCleanup(env, "test", manifest);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        assertTrue(manifestAndMetadata.v1().isEmpty());
        Metadata metadata = manifestAndMetadata.v2();
        assertTrue(Metadata.isGlobalStateEquals(metadata, Metadata.EMPTY_METADATA));
    }

    public void testLoadFullStateMissingGlobalMetadata() throws IOException {
        IndexMetadata index = indexMetadata("test1");
        long indexGeneration = MetaStateWriterUtils.writeIndex(env, "test", index);
        Manifest manifest = new Manifest(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            Manifest.empty().globalGeneration(),
            new HashMap<Index, Long>() {
                {
                    put(index.getIndex(), indexGeneration);
                }
            }
        );
        assertTrue(manifest.isGlobalGenerationMissing());
        MetaStateWriterUtils.writeManifestAndCleanup(env, "test", manifest);

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

        long globalGeneration = MetaStateWriterUtils.writeGlobalState(env, "first global state write", metadata);
        long indexGeneration = MetaStateWriterUtils.writeIndex(env, "first index state write", index);

        Manifest manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(), globalGeneration, new HashMap<Index, Long>() {
            {
                put(index.getIndex(), indexGeneration);
            }
        });
        MetaStateWriterUtils.writeManifestAndCleanup(env, "first manifest write", manifest);

        Metadata newMetadata = Metadata.builder()
            .persistentSettings(Settings.builder().put("test1", "value2").build())
            .put(index, true)
            .build();
        globalGeneration = MetaStateWriterUtils.writeGlobalState(env, "second global state write", newMetadata);

        Tuple<Manifest, Metadata> manifestAndMetadata = metaStateService.loadFullState();
        assertThat(manifestAndMetadata.v1(), equalTo(manifest));

        Metadata loadedMetadata = manifestAndMetadata.v2();
        assertThat(loadedMetadata.persistentSettings(), equalTo(metadata.persistentSettings()));
        assertThat(loadedMetadata.hasIndex("test1"), equalTo(true));
        assertThat(loadedMetadata.index("test1"), equalTo(index));

        manifest = new Manifest(randomNonNegativeLong(), randomNonNegativeLong(), globalGeneration, new HashMap<Index, Long>() {
            {
                put(index.getIndex(), indexGeneration);
            }
        });

        MetaStateWriterUtils.writeManifestAndCleanup(env, "second manifest write", manifest);
        boolean useFsync = IndexModule.NODE_STORE_USE_FSYNC.get(env.settings());
        Metadata.FORMAT.cleanupOldFiles(globalGeneration, useFsync, env.nodeDataPaths());
        IndexMetadata.FORMAT.cleanupOldFiles(indexGeneration, useFsync, env.indexPaths(index.getIndex()));

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

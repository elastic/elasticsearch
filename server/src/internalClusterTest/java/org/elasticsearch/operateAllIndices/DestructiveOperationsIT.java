/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operateAllIndices;

import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.APIBlock.WRITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class DestructiveOperationsIT extends ESIntegTestCase {

    @After
    public void afterTest() {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), (String) null));
    }

    public void testDeleteIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));
        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareDelete("1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareDelete("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareDelete("i*").get());
        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareDelete("_all").get());
    }

    public void testDeleteIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareDelete("_all").get());
        } else {
            assertAcked(indicesAdmin().prepareDelete("*").get());
        }

        assertThat(indexExists("_all"), equalTo(false));
    }

    public void testCloseIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareClose("1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareClose("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareClose("i*").get());
        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareClose("_all").get());
    }

    public void testCloseIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("_all").get());
        } else {
            assertAcked(indicesAdmin().prepareClose("*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.CLOSE, indexMetadataEntry.getValue().getState());
        }
    }

    public void testOpenIndexIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");
        assertAcked(indicesAdmin().prepareClose("1index", "index1").get());

        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareOpen("*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareOpen("i*").get());
        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareOpen("_all").get());
    }

    public void testOpenIndexDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");
        assertAcked(indicesAdmin().prepareClose("1index", "index1").get());

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareOpen("_all").get());
        } else {
            assertAcked(indicesAdmin().prepareOpen("*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        for (Map.Entry<String, IndexMetadata> indexMetadataEntry : state.getMetadata().indices().entrySet()) {
            assertEquals(IndexMetadata.State.OPEN, indexMetadataEntry.getValue().getState());
        }
    }

    public void testAddIndexBlockIsRejected() throws Exception {
        updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(indicesAdmin().prepareAddBlock(WRITE, "1index").get());
        // Special "match none" pattern succeeds, since non-destructive
        assertAcked(indicesAdmin().prepareAddBlock(WRITE, "*", "-*").get());

        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareAddBlock(WRITE, "i*").get());
        expectThrows(IllegalArgumentException.class, () -> indicesAdmin().prepareAddBlock(WRITE, "_all").get());
    }

    public void testAddIndexBlockDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            updateClusterSettings(Settings.builder().put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false));
        }

        createIndex("index1", "1index");

        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareAddBlock(WRITE, "_all").get());
        } else {
            assertAcked(indicesAdmin().prepareAddBlock(WRITE, "*").get());
        }

        ClusterState state = clusterAdmin().prepareState().get().getState();
        assertTrue("write block is set on index1", state.getBlocks().hasIndexBlock("index1", IndexMetadata.INDEX_WRITE_BLOCK));
        assertTrue("write block is set on 1index", state.getBlocks().hasIndexBlock("1index", IndexMetadata.INDEX_WRITE_BLOCK));
    }
}

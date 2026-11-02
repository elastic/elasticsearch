/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Resize targets record their source by name and UUID in their settings and recover from the source's shards on the same node.
 * A rename of the source before or during a resize must not break that.
 */
public class RenameIndexResizeIT extends ESIntegTestCase {

    public void testResizeFromRenamedSource() throws Exception {
        internalCluster().startNode();
        createIndex("source", indexSettings(2, 0).build(), false);
        final int docs = randomIntBetween(1, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setId(String.valueOf(i)).setSource("f", i).get();
        }
        refresh("source");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "source");

        rename("source", "renamed");

        final ResizeType type = randomFrom(ResizeType.CLONE, ResizeType.SHRINK, ResizeType.SPLIT);
        final Settings.Builder targetSettings = Settings.builder().putNull(IndexMetadata.SETTING_BLOCKS_WRITE);
        if (type == ResizeType.SHRINK) {
            targetSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        } else if (type == ResizeType.SPLIT) {
            targetSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4);
        }
        assertAcked(executeResize(type, "renamed", "target", targetSettings));
        ensureGreen("target");
        ElasticsearchAssertions.assertHitCount(prepareSearch("target").setSize(0), docs);
        final var targetSettingsResponse = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, "target").get();
        assertThat(targetSettingsResponse.getSetting("target", IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY), equalTo("renamed"));
    }

    public void testRenameSourceWhileResizeTargetRecovers() throws Exception {
        internalCluster().startNode();
        createIndex("source", indexSettings(2, 0).build(), false);
        final int docs = randomIntBetween(1, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setId(String.valueOf(i)).setSource("f", i).get();
        }
        refresh("source");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "source");

        // rename the source right after the target is created, while its shards may still be recovering from the source
        assertAcked(executeResize(ResizeType.CLONE, "source", "target", Settings.builder().putNull(IndexMetadata.SETTING_BLOCKS_WRITE)));
        rename("source", "renamed");
        ensureGreen("target", "renamed");
        ElasticsearchAssertions.assertHitCount(prepareSearch("target").setSize(0), docs);
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), docs);
        // the target still records the source by the name it had when the resize started
        final var targetSettingsResponse = indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, "target").get();
        assertThat(targetSettingsResponse.getSetting("target", IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY), equalTo("source"));
    }

    private void rename(String source, String target) {
        final var response = client().execute(
            RenameIndexAction.INSTANCE,
            new RenameIndexAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, source, target)
        ).actionGet();
        assertTrue(response.isAcknowledged());
    }
}

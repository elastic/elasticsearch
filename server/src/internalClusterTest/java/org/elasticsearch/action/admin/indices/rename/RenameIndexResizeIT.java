/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rename;

import org.elasticsearch.action.admin.indices.alias.TransportIndicesAliasesAction;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import static org.elasticsearch.action.admin.indices.ResizeIndexTestUtils.executeResize;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/**
 * Resize targets record their source by name and UUID in their settings and recover from the source's shards on the same node.
 * A rename of the source before or during a resize must not break that.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class RenameIndexResizeIT extends ESIntegTestCase {

    public void testResizeFromRenamedSource() throws Exception {
        createIndex("source", indexSettings(2, 0).build(), false);
        final int docs = randomIntBetween(1, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setId(String.valueOf(i)).setSource("f", i).get();
        }
        refresh("source");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "source");

        rename("source", "renamed");

        final ResizeType type = randomFrom(ResizeType.CLONE, ResizeType.SHRINK, ResizeType.SPLIT);
        // the test index template may add replicas, which a one-node cluster cannot allocate
        final Settings.Builder targetSettings = Settings.builder()
            .putNull(IndexMetadata.SETTING_BLOCKS_WRITE)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        if (type == ResizeType.SHRINK) {
            targetSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        } else if (type == ResizeType.SPLIT) {
            targetSettings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4);
        }
        assertAcked(executeResize(type, "renamed", "target", targetSettings));
        ensureGreen("target");
        ElasticsearchAssertions.assertHitCount(prepareSearch("target").setSize(0), docs);
    }

    public void testRenameSourceWhileResizeTargetRecovers() throws Exception {
        createIndex("source", indexSettings(2, 0).build(), false);
        final int docs = randomIntBetween(1, 20);
        for (int i = 0; i < docs; i++) {
            prepareIndex("source").setId(String.valueOf(i)).setSource("f", i).get();
        }
        refresh("source");
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true), "source");

        // rename the source right after the target is created, while its shards may still be recovering from the source
        assertAcked(
            executeResize(
                ResizeType.CLONE,
                "source",
                "target",
                Settings.builder().putNull(IndexMetadata.SETTING_BLOCKS_WRITE).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );
        rename("source", "renamed");
        ensureGreen("target", "renamed");
        ElasticsearchAssertions.assertHitCount(prepareSearch("target").setSize(0), docs);
        ElasticsearchAssertions.assertHitCount(prepareSearch("renamed").setSize(0), docs);
    }

    private void rename(String source, String target) {
        final var response = client().execute(TransportIndicesAliasesAction.TYPE, renameIndexRequest(source, target)).actionGet();
        assertTrue(response.isAcknowledged());
    }
}

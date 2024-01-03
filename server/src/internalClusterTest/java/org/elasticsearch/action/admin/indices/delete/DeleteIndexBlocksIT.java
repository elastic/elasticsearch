/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class DeleteIndexBlocksIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false) // we control the read-only-allow-delete block
            .build();
    }

    public void testDeleteIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");
        try {
            setClusterReadOnly(true);
            assertBlocked(indicesAdmin().prepareDelete("test"), Metadata.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testDeleteIndexOnIndexReadOnlyAllowDeleteSetting() {
        createIndex("test");
        ensureGreen("test");
        IndexRequestBuilder indexRequestBuilder = prepareIndex("test").setId("1").setSource("foo", "bar");
        indexRequestBuilder.get();
        indexRequestBuilder.request().decRef();
        refresh();
        try {
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true), "test");
            assertSearchHits(prepareSearch(), "1");
            indexRequestBuilder = prepareIndex("test").setId("2").setSource("foo", "bar");
            assertBlocked(indexRequestBuilder, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            indexRequestBuilder.request().decRef();
            assertSearchHits(prepareSearch(), "1");
            assertAcked(indicesAdmin().prepareDelete("test"));
        } finally {
            Settings settings = Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build();
            assertAcked(
                indicesAdmin().prepareUpdateSettings("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).setSettings(settings)
            );
        }
    }

    public void testClusterBlockMessageHasIndexName() {
        try {
            createIndex("test");
            ensureGreen("test");
            updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true), "test");
            IndexRequestBuilder indexRequestBuilder = prepareIndex("test").setId("1").setSource("foo", "bar");
            ClusterBlockException e = expectThrows(ClusterBlockException.class, indexRequestBuilder);
            indexRequestBuilder.request().decRef();
            assertEquals(
                "index [test] blocked by: [TOO_MANY_REQUESTS/12/disk usage exceeded flood-stage watermark, "
                    + "index has read-only-allow-delete block];",
                e.getMessage()
            );
        } finally {
            updateIndexSettings(Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE), "test");
        }
    }

    public void testDeleteIndexOnClusterReadOnlyAllowDeleteSetting() {
        createIndex("test");
        ensureGreen("test");
        IndexRequestBuilder indexRequestBuilder = prepareIndex("test").setId("1").setSource("foo", "bar");
        indexRequestBuilder.get();
        indexRequestBuilder.request().decRef();
        refresh();
        try {
            updateClusterSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true));
            assertSearchHits(prepareSearch(), "1");
            indexRequestBuilder = prepareIndex("test").setId("2").setSource("foo", "bar");
            assertBlocked(indexRequestBuilder, Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            indexRequestBuilder.request().decRef();
            assertBlocked(
                indicesAdmin().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.number_of_replicas", 2)),
                Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK
            );
            assertSearchHits(prepareSearch(), "1");
            assertAcked(indicesAdmin().prepareDelete("test"));
        } finally {
            updateClusterSettings(Settings.builder().putNull(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey()));
        }
    }
}

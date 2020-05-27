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

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHits;

public class DeleteIndexBlocksIT extends ESIntegTestCase {
    public void testDeleteIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");
        try {
            setClusterReadOnly(true);
            assertBlocked(client().admin().indices().prepareDelete("test"), Metadata.CLUSTER_READ_ONLY_BLOCK);
        } finally {
            setClusterReadOnly(false);
        }
    }

    public void testDeleteIndexOnIndexReadOnlyAllowDeleteSetting() {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get();
        refresh();
        try {
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build();
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get());
            assertSearchHits(client().prepareSearch().get(), "1");
            assertBlocked(client().prepareIndex().setIndex("test").setId("2").setSource("foo", "bar"),
                IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertBlocked(client().admin().indices().prepareUpdateSettings("test")
                    .setSettings(Settings.builder().put("index.number_of_replicas", 2)), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertSearchHits(client().prepareSearch().get(), "1");
            assertAcked(client().admin().indices().prepareDelete("test"));
        } finally {
            Settings settings = Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build();
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setIndicesOptions(IndicesOptions.lenientExpandOpen()).
                setSettings(settings).get());
        }
    }

    public void testClusterBlockMessageHasIndexName() {
        try {
            createIndex("test");
            ensureGreen("test");
            Settings settings = Settings.builder().put(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE, true).build();
            client().admin().indices().prepareUpdateSettings("test").setSettings(settings).get();
            ClusterBlockException e = expectThrows(ClusterBlockException.class, () ->
                client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get());
            assertEquals("index [test] blocked by: [TOO_MANY_REQUESTS/12/index read-only / allow delete (api)];", e.getMessage());
        } finally {
            assertAcked(client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().putNull(IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE).build()).get());
        }
    }

    public void testDeleteIndexOnClusterReadOnlyAllowDeleteSetting() {
        createIndex("test");
        ensureGreen("test");
        client().prepareIndex().setIndex("test").setId("1").setSource("foo", "bar").get();
        refresh();
        try {
            Settings settings = Settings.builder().put(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey(), true).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
            assertSearchHits(client().prepareSearch().get(), "1");
            assertBlocked(client().prepareIndex().setIndex("test").setId("2").setSource("foo", "bar"),
                Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertBlocked(client().admin().indices().prepareUpdateSettings("test")
                .setSettings(Settings.builder().put("index.number_of_replicas", 2)), Metadata.CLUSTER_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertSearchHits(client().prepareSearch().get(), "1");
            assertAcked(client().admin().indices().prepareDelete("test"));
        } finally {
            Settings settings = Settings.builder().putNull(Metadata.SETTING_READ_ONLY_ALLOW_DELETE_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
        }
    }
}

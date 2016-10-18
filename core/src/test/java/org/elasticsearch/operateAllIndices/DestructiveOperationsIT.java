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

package org.elasticsearch.operateAllIndices;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class DestructiveOperationsIT extends ESIntegTestCase {

    @After
    public void afterTest() {
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), (String)null)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
    }

    public void testDeleteIndexDestructiveOperationsRequireName() throws Exception {
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        createIndex("index1", "1index");

        // Should succeed, since no wildcards
        assertAcked(client().admin().indices().prepareDelete("1index"));

        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareDelete("*").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareDelete("i*").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareDelete("_all").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareDelete("*", "-index1").get());
    }

    public void testDeleteIndexDestructiveOperationsDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            Settings settings = Settings.builder()
                    .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false)
                    .build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        }
        createIndex("index1", "1index");
        assertAcked(client().admin().indices().prepareDelete("_all"));
        assertThat(client().admin().indices().prepareExists("index1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("1index").get().isExists(), equalTo(false));

        createIndex("index1", "1index");
        assertAcked(client().admin().indices().prepareDelete("*"));
        assertThat(client().admin().indices().prepareExists("index1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("1index").get().isExists(), equalTo(false));

        createIndex("index1", "1index");
        assertAcked(client().admin().indices().prepareDelete("i*"));
        assertThat(client().admin().indices().prepareExists("index1").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("1index").get().isExists(), equalTo(true));

        createIndex("index1");
        assertAcked(client().admin().indices().prepareDelete("*", "-index1"));
        assertThat(client().admin().indices().prepareExists("1index").get().isExists(), equalTo(false));
        assertThat(client().admin().indices().prepareExists("index1").get().isExists(), equalTo(true));
    }

    public void testOpenCloseIndexDestructiveOperationsRequireName() throws Exception {
        Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), true)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));

        createIndex("index1", "1index");
        // Should succeed, since no wildcards
        assertAcked(client().admin().indices().prepareClose("1index"));

        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareClose("_all").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareClose("*").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareClose("i*").get());
        expectThrows(IllegalArgumentException.class, () -> client().admin().indices().prepareClose("*", "-index1").get());
        expectThrows(IllegalArgumentException.class, () -> assertAcked(client().admin().indices().prepareOpen("_all").get()));
        expectThrows(IllegalArgumentException.class, () -> assertAcked(client().admin().indices().prepareOpen("*").get()));
        expectThrows(IllegalArgumentException.class, () -> assertAcked(client().admin().indices().prepareOpen("i*").get()));
        expectThrows(IllegalArgumentException.class, () -> assertAcked(client().admin().indices().prepareOpen("*", "-index1").get()));
    }

    public void testOpenCloseIndexDestructiveOperationsDefaultBehaviour() throws Exception {
        if (randomBoolean()) {
            Settings settings = Settings.builder()
                    .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), false)
                    .build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        }
        createIndex("index1", "1index");
        assertAcked(client().admin().indices().prepareClose("_all"));
        checkIndexState(IndexMetaData.State.CLOSE, "index1", "1index");
        assertAcked(client().admin().indices().prepareOpen("_all"));
        checkIndexState(IndexMetaData.State.OPEN, "index1", "1index");
        assertAcked(client().admin().indices().prepareClose("*"));
        checkIndexState(IndexMetaData.State.CLOSE, "index1", "1index");
        assertAcked(client().admin().indices().prepareOpen("*"));
        checkIndexState(IndexMetaData.State.OPEN, "index1", "1index");
        assertAcked(client().admin().indices().prepareClose("i*"));
        checkIndexState(IndexMetaData.State.CLOSE, "index1");
        checkIndexState(IndexMetaData.State.OPEN, "1index");
        assertAcked(client().admin().indices().prepareOpen("i*"));
        checkIndexState(IndexMetaData.State.OPEN, "index1", "1index");
        assertAcked(client().admin().indices().prepareClose("*", "-index1"));
        checkIndexState(IndexMetaData.State.OPEN, "index1");
        checkIndexState(IndexMetaData.State.CLOSE, "1index");
        assertAcked(client().admin().indices().prepareOpen("*", "-index1"));
        checkIndexState(IndexMetaData.State.OPEN, "index1", "1index");
    }

    private void checkIndexState(IndexMetaData.State state, String... indices) {
        ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
        for (String index : indices) {
            IndexMetaData indexMetaData = clusterStateResponse.getState().metaData().indices().get(index);
            assertThat(indexMetaData, notNullValue());
            assertThat(indexMetaData.getState(), equalTo(state));
        }
    }
}

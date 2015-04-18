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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.IsNull.notNullValue;

@ClusterScope(scope = Scope.TEST)
public class CreateIndexTests extends ElasticsearchIntegrationTest{

    @Test
    public void testCreationDate_Given() {
        prepareCreate("test").setSettings(ImmutableSettings.builder().put(IndexMetaData.SETTING_CREATION_DATE, 4l)).get();
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        MetaData metadata = state.getMetaData();
        assertThat(metadata, notNullValue());
        ImmutableOpenMap<String, IndexMetaData> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetaData index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.creationDate(), equalTo(4l));
    }

    @Test
    public void testCreationDate_Generated() {
        long timeBeforeRequest = System.currentTimeMillis();
        prepareCreate("test").get();
        long timeAfterRequest = System.currentTimeMillis();
        ClusterStateResponse response = client().admin().cluster().prepareState().get();
        ClusterState state = response.getState();
        assertThat(state, notNullValue());
        MetaData metadata = state.getMetaData();
        assertThat(metadata, notNullValue());
        ImmutableOpenMap<String, IndexMetaData> indices = metadata.getIndices();
        assertThat(indices, notNullValue());
        assertThat(indices.size(), equalTo(1));
        IndexMetaData index = indices.get("test");
        assertThat(index, notNullValue());
        assertThat(index.creationDate(), allOf(lessThanOrEqualTo(timeAfterRequest), greaterThanOrEqualTo(timeBeforeRequest)));
    }

    @Test
    public void testDoubleAddMapping() throws Exception {
        try {
            prepareCreate("test")
                    .addMapping("type1", "date", "type=date")
                    .addMapping("type1", "num", "type=integer");
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", new HashMap<String,Object>())
                    .addMapping("type1", new HashMap<String,Object>());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
        try {
            prepareCreate("test")
                    .addMapping("type1", jsonBuilder())
                    .addMapping("type1", jsonBuilder());
            fail("did not hit expected exception");
        } catch (IllegalStateException ise) {
            // expected
        }
    }

    @Test
    public void testInvalidShardCountSettings() throws Exception {
        try {
            prepareCreate("test").setSettings(ImmutableSettings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(-10, 0))
                    .build())
            .get();
            fail("should have thrown an exception about the primary shard count");
        } catch (ActionRequestValidationException e) {
            assertThat("message contains error about shard count: " + e.getMessage(),
                    e.getMessage().contains("index must have 1 or more primary shards"), equalTo(true));
        }

        try {
            prepareCreate("test").setSettings(ImmutableSettings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(-10, -1))
                    .build())
                    .get();
            fail("should have thrown an exception about the replica shard count");
        } catch (ActionRequestValidationException e) {
            assertThat("message contains error about shard count: " + e.getMessage(),
                    e.getMessage().contains("index must have 0 or more replica shards"), equalTo(true));
        }

        try {
            prepareCreate("test").setSettings(ImmutableSettings.builder()
                    .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(-10, 0))
                    .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(-10, -1))
                    .build())
                    .get();
            fail("should have thrown an exception about the shard count");
        } catch (ActionRequestValidationException e) {
            assertThat("message contains error about shard count: " + e.getMessage(),
                    e.getMessage().contains("index must have 1 or more primary shards"), equalTo(true));
            assertThat("message contains error about shard count: " + e.getMessage(),
                    e.getMessage().contains("index must have 0 or more replica shards"), equalTo(true));
        }
    }
}

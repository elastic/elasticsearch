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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
public class MetaStateServiceTests extends ElasticsearchTestCase {

    private static Settings indexSettings = ImmutableSettings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    @Test
    public void testWriteLoadIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(randomSettings(), env);

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            metaStateService.writeIndex("test_write", index, null);
            assertThat(metaStateService.loadIndexState("test1"), equalTo(index));
        }
    }

    @Test
    public void testLoadMissingIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(randomSettings(), env);
            assertThat(metaStateService.loadIndexState("test1"), nullValue());
        }
    }

    @Test
    public void testWriteLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(randomSettings(), env);

            MetaData metaData = MetaData.builder()
                    .persistentSettings(ImmutableSettings.builder().put("test1", "value1").build())
                    .build();
            metaStateService.writeGlobalState("test_write", metaData);
            assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
        }
    }

    @Test
    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(randomSettings(), env);

            MetaData metaData = MetaData.builder()
                    .persistentSettings(ImmutableSettings.builder().put("test1", "value1").build())
                    .build();
            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaDataWithIndex = MetaData.builder(metaData).put(index, true).build();

            metaStateService.writeGlobalState("test_write", metaDataWithIndex);
            assertThat(metaStateService.loadGlobalState().persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(metaStateService.loadGlobalState().hasIndex("test1"), equalTo(false));
        }
    }

    @Test
    public void tesLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(randomSettings(), env);

            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings).build();
            MetaData metaData = MetaData.builder()
                    .persistentSettings(ImmutableSettings.builder().put("test1", "value1").build())
                    .put(index, true)
                    .build();

            metaStateService.writeGlobalState("test_write", metaData);
            metaStateService.writeIndex("test_write", index, null);

            MetaData loadedState = metaStateService.loadFullState();
            assertThat(loadedState.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedState.hasIndex("test1"), equalTo(true));
            assertThat(loadedState.index("test1"), equalTo(index));
        }
    }

    private Settings randomSettings() {
        ImmutableSettings.Builder builder = ImmutableSettings.builder();
        if (randomBoolean()) {
            builder.put(MetaStateService.FORMAT_SETTING, randomFrom(XContentType.values()).shortName());
        }
        return builder.build();
    }
}

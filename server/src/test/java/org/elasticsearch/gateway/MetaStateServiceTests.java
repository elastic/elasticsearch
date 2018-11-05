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
import org.elasticsearch.cluster.metadata.MetaState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class MetaStateServiceTests extends ESTestCase {
    private static Settings indexSettings() {
        return Settings.builder()
                .put(IndexMetaData.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
    }

    public void testWriteLoadGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            metaStateService.writeGlobalState("test_write", metaData);
            metaStateService.writeMetaState("test");
            assertThat(metaStateService.getMetaData().persistentSettings(), equalTo(metaData.persistentSettings()));
        }
    }

    public void testWriteGlobalStateWithIndexAndNoIndexIsLoaded() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());

            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            IndexMetaData index = IndexMetaData.builder("test1").settings(indexSettings()).build();
            MetaData metaDataWithIndex = MetaData.builder(metaData).put(index, true).build();

            metaStateService.writeGlobalState("test_write", metaDataWithIndex);
            metaStateService.writeMetaState("test");

            MetaData loadedMetaData = metaStateService.getMetaData();
            assertThat(loadedMetaData.persistentSettings(), equalTo(metaData.persistentSettings()));
            assertThat(loadedMetaData.hasIndex("test1"), equalTo(false));
        }
    }

    public void testWriteLoadIndex() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();

            metaStateService.writeGlobalState("test_write", metaData);

            IndexMetaData index = IndexMetaData.builder("index1").settings(indexSettings()).build();
            metaStateService.writeIndex("test_write_index", index);

            metaStateService.writeMetaState("test");

            assertThat(metaStateService.getMetaData().index("index1"), equalTo(index));
        }
    }

    public void testOverwriteGlobal() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();

            metaStateService.writeGlobalState("test_write1", metaData);
            metaStateService.writeMetaState("test1");

            MetaData newMetaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value2").build())
                    .build();

            metaStateService.writeGlobalState("test_write2", newMetaData);
            metaStateService.writeMetaState("test2");

            assertTrue(MetaData.isGlobalStateEquals(metaStateService.getMetaData(), newMetaData));
        }
    }

    public void testIndices() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            MetaData metaData = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            IndexMetaData notChangedIndex = IndexMetaData.builder("not_changed_index").settings(indexSettings()).build();
            logger.error(notChangedIndex.getIndexUUID());
            IndexMetaData removedIndex = IndexMetaData.builder("removed_index").settings(indexSettings()).build();

            IndexMetaData changedIndex_v1 = IndexMetaData.builder("changed_index").settings(indexSettings()).build();
            IndexMetaData changedIndex_v2 = IndexMetaData.builder(changedIndex_v1).version(changedIndex_v1.getVersion() + 1).build();

            IndexMetaData newIndex = IndexMetaData.builder("new_index").settings(indexSettings()).build();

            metaStateService.writeGlobalState("write1", metaData);
            metaStateService.writeIndex("write1", notChangedIndex);
            metaStateService.writeIndex("write1", removedIndex);
            metaStateService.writeIndex("write1", changedIndex_v1);
            metaStateService.writeMetaState("write1");

            metaStateService.keepGlobalState();
            metaStateService.keepIndex(notChangedIndex.getIndex());
            metaStateService.writeIndex("write2", changedIndex_v2);
            metaStateService.writeIndex("write2", newIndex);
            metaStateService.writeMetaState("write2");

            MetaData loadedMetaData = metaStateService.getMetaData();
            assertTrue(MetaData.isGlobalStateEquals(loadedMetaData, metaData));
            assertThat(loadedMetaData.index("not_changed_index"), equalTo(notChangedIndex));
            assertThat(loadedMetaData.index("removed_index"), is(nullValue()));
            assertThat(loadedMetaData.index("changed_index"), equalTo(changedIndex_v2));
            assertThat(loadedMetaData.index("new_index"), equalTo(newIndex));
        }
    }

    public void testLoadZen1BWC() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            MetaData metaData_v1 = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value1").build())
                    .build();
            MetaData metaData_v2 = MetaData.builder()
                    .persistentSettings(Settings.builder().put("test1", "value2").build())
                    .build();

            IndexMetaData index_v1 = IndexMetaData.builder("index").settings(indexSettings()).build();
            IndexMetaData index_v2 = IndexMetaData.builder(index_v1).version(index_v1.getVersion() + 1).build();

            metaStateService.writeGlobalState("write1", metaData_v1);
            metaStateService.writeIndex("write1", index_v1);
            metaStateService.writeMetaState("write1");

            metaStateService.writeGlobalState("write2", metaData_v2);
            metaStateService.writeIndex("write2", index_v2);
            //we don't write manifest file here

            metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            MetaData loadedMetaData = metaStateService.getMetaData(); //this must load old metadata
            assertTrue(MetaData.isGlobalStateEquals(loadedMetaData, metaData_v1));
            assertThat(loadedMetaData.index("index"), equalTo(index_v1));

            MetaState.FORMAT.cleanupOldFiles(Long.MAX_VALUE, env.nodeDataPaths()); // this will erase manifest file
            metaStateService = new MetaStateService(Settings.EMPTY, env, xContentRegistry());
            loadedMetaData = metaStateService.getMetaData(); //this must load new metadata, because manifest file is gone
            assertTrue(MetaData.isGlobalStateEquals(loadedMetaData, metaData_v2));
            assertThat(loadedMetaData.index("index"), equalTo(index_v2));
        }
    }


}

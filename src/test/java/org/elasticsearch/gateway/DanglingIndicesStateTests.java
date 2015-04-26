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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 */
public class DanglingIndicesStateTests extends ElasticsearchTestCase {

    private static Settings indexSettings = ImmutableSettings.builder()
            .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();

    @Test
    public void testCleanupWhenEmpty() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(ImmutableSettings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(ImmutableSettings.EMPTY, env, metaStateService, null);

            assertTrue(danglingState.getDanglingIndices().isEmpty());
            MetaData metaData = MetaData.builder().build();
            danglingState.cleanupAllocatedDangledIndices(metaData);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

    @Test
    public void testDanglingProcessing() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(ImmutableSettings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(ImmutableSettings.EMPTY, env, metaStateService, null);

            MetaData metaData = MetaData.builder().build();

            IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(indexSettings).build();
            metaStateService.writeIndex("test_write", dangledIndex, null);

            // check that several runs when not in the metadata still keep the dangled index around
            int numberOfChecks = randomIntBetween(1, 10);
            for (int i = 0; i < numberOfChecks; i++) {
                Map<String, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData);
                assertThat(newDanglingIndices.size(), equalTo(1));
                assertThat(newDanglingIndices.keySet(), Matchers.hasItems("test1"));
                assertTrue(danglingState.getDanglingIndices().isEmpty());
            }

            for (int i = 0; i < numberOfChecks; i++) {
                danglingState.findNewAndAddDanglingIndices(metaData);

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems("test1"));
            }

            // simulate allocation to the metadata
            metaData = MetaData.builder(metaData).put(dangledIndex, true).build();

            // check that several runs when in the metadata, but not cleaned yet, still keeps dangled
            for (int i = 0; i < numberOfChecks; i++) {
                Map<String, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData);
                assertTrue(newDanglingIndices.isEmpty());

                assertThat(danglingState.getDanglingIndices().size(), equalTo(1));
                assertThat(danglingState.getDanglingIndices().keySet(), Matchers.hasItems("test1"));
            }

            danglingState.cleanupAllocatedDangledIndices(metaData);
            assertTrue(danglingState.getDanglingIndices().isEmpty());
        }
    }

    @Test
    public void testRenameOfIndexState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            MetaStateService metaStateService = new MetaStateService(ImmutableSettings.EMPTY, env);
            DanglingIndicesState danglingState = new DanglingIndicesState(ImmutableSettings.EMPTY, env, metaStateService, null);

            MetaData metaData = MetaData.builder().build();

            IndexMetaData dangledIndex = IndexMetaData.builder("test1").settings(indexSettings).build();
            metaStateService.writeIndex("test_write", dangledIndex, null);

            for (Path path : env.indexPaths(new Index("test1"))) {
                Files.move(path, path.getParent().resolve("test1_renamed"));
            }

            Map<String, IndexMetaData> newDanglingIndices = danglingState.findNewDanglingIndices(metaData);
            assertThat(newDanglingIndices.size(), equalTo(1));
            assertThat(newDanglingIndices.keySet(), Matchers.hasItems("test1_renamed"));
        }
    }
}

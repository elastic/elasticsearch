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

package org.elasticsearch.indices.memory;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;

import static org.hamcrest.Matchers.*;

public class IndexingMemoryControllerUpdateSettingsTest extends ElasticsearchSingleNodeTest {
    public void testDynamicSettings() {
        IndexingMemoryController controller = getInstanceFromNode(IndexingMemoryController.class);
        Settings settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.INDEX_BUFFER_SIZE, "42mb")
            .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE, "1mb")
            .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE, "2mb")
            .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE, "17mb")
            .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE, "47mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        assertThat(controller.getMinShardIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("1mb")));
        assertThat(controller.getMaxShardIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("2mb")));
        assertThat(controller.getMinIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("17mb")));
        assertThat(controller.getMaxIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("47mb")));
        assertThat(controller.getIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("42mb")));
    }

    public void testInvalidMinMaxShard() {
        IndexingMemoryController controller = getInstanceFromNode(IndexingMemoryController.class);
        ByteSizeValue minBefore = controller.getMinShardIndexingBufferSize();
        ByteSizeValue maxBefore = controller.getMaxShardIndexingBufferSize();
        assertThat(minBefore.bytes(), lessThan(maxBefore.bytes()));
        Settings settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE, "2mb")
            .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE, "1mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();

        // Nothing should have changed since the request was invalid:
        assertThat(controller.getMinShardIndexingBufferSize(), is(minBefore));
        assertThat(controller.getMaxShardIndexingBufferSize(), is(maxBefore));

        // Put valid settings back:
        settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.MIN_SHARD_INDEX_BUFFER_SIZE, "1mb")
            .put(IndexingMemoryController.MAX_SHARD_INDEX_BUFFER_SIZE, "2mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
    }

    public void testInvalidMinMax() {
        IndexingMemoryController controller = getInstanceFromNode(IndexingMemoryController.class);
        ByteSizeValue minBefore = controller.getMinIndexingBufferSize();
        ByteSizeValue maxBefore = controller.getMaxIndexingBufferSize();
        Settings settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE, "20mb")
            .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE, "10mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();

        // Nothing should have changed since the request was invalid:
        assertThat(controller.getMinIndexingBufferSize(), is(minBefore));
        assertThat(controller.getMaxIndexingBufferSize(), is(maxBefore));

        // Put valid settings back:
        settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE, "17mb")
            .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE, "47mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
    }

    // With index_buffer_size at 20% of heap, change just min & max and confirm the 20% "sticks":
    public void testChangeOnlyMinMax() {
        IndexingMemoryController controller = getInstanceFromNode(IndexingMemoryController.class);
        ByteSizeValue minBefore = controller.getMinIndexingBufferSize();
        ByteSizeValue maxBefore = controller.getMaxIndexingBufferSize();
        Settings settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.INDEX_BUFFER_SIZE, "20%")
            .put(IndexingMemoryController.MIN_INDEX_BUFFER_SIZE, "20mb")
            .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE, "40mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();

        ByteSizeValue indexBufferBefore = controller.getIndexingBufferSize();
        assertThat(controller.getIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("40mb")));
        settings = ImmutableSettings.builder()
            .put(IndexingMemoryController.MAX_INDEX_BUFFER_SIZE, "400mb")
            .build();
        client().admin().cluster().prepareUpdateSettings().setPersistentSettings(settings).get();
        // Actual indexing buffer should have increased, since we increased the max:
        assertThat(controller.getIndexingBufferSize().bytes(), greaterThan(indexBufferBefore.bytes()));
        // Min should still be 20mb:
        assertThat(controller.getMinIndexingBufferSize(), is(ByteSizeValue.parseBytesSizeValue("20mb")));
    }
}

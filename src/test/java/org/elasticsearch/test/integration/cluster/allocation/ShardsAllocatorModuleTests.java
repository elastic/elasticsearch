package org.elasticsearch.test.integration.cluster.allocation;

/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.EvenShardsCountAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocatorModule;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.junit.After;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.instanceOf;

public class ShardsAllocatorModuleTests extends AbstractNodesTests {

    @After
    public void cleanAndCloseNodes() throws Exception {
        closeAllNodes();
    }

    public void testLoadDefaultShardsAllocator() {
        assertAllocatorInstance(ImmutableSettings.Builder.EMPTY_SETTINGS, BalancedShardsAllocator.class);
    }

    public void testLoadByShortKeyShardsAllocator() {
        Settings build = settingsBuilder().put(ShardsAllocatorModule.TYPE_KEY, ShardsAllocatorModule.EVEN_SHARD_COUNT_ALLOCATOR_KEY)
                .build();
        assertAllocatorInstance(build, EvenShardsCountAllocator.class);
        build = settingsBuilder().put(ShardsAllocatorModule.TYPE_KEY, ShardsAllocatorModule.BALANCED_ALLOCATOR_KEY).build();
        assertAllocatorInstance(build, BalancedShardsAllocator.class);
    }

    public void testLoadByClassNameShardsAllocator() {
        Settings build = settingsBuilder().put(ShardsAllocatorModule.TYPE_KEY, "EvenShardsCount").build();
        assertAllocatorInstance(build, EvenShardsCountAllocator.class);

        build = settingsBuilder().put(ShardsAllocatorModule.TYPE_KEY,
                "org.elasticsearch.cluster.routing.allocation.allocator.EvenShardsCountAllocator").build();
        assertAllocatorInstance(build, EvenShardsCountAllocator.class);
    }

    private void assertAllocatorInstance(Settings settings, Class<? extends ShardsAllocator> clazz) {
        closeNode("node");
        Node _node = startNode("node", settings);
        InternalNode node = (InternalNode) _node;
        ShardsAllocator instance = node.injector().getInstance(ShardsAllocator.class);
        node.close();
        assertThat(instance, instanceOf(clazz));
    }
}

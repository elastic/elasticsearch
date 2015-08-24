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

package org.elasticsearch.cluster.allocation;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.io.IOException;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.ESIntegTestCase.*;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope= Scope.TEST, numDataNodes =0)
public class ShardsAllocatorModuleIT extends ESIntegTestCase {

    public void testLoadDefaultShardsAllocator() throws IOException {
        assertAllocatorInstance(Settings.Builder.EMPTY_SETTINGS, BalancedShardsAllocator.class);
    }

    public void testLoadByShortKeyShardsAllocator() throws IOException {
        Settings build = settingsBuilder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_KEY, "even_shard") // legacy just to make sure we don't barf
                .build();
        assertAllocatorInstance(build, BalancedShardsAllocator.class);
        build = settingsBuilder().put(ClusterModule.SHARDS_ALLOCATOR_TYPE_KEY, ClusterModule.BALANCED_ALLOCATOR).build();
        assertAllocatorInstance(build, BalancedShardsAllocator.class);
    }

    private void assertAllocatorInstance(Settings settings, Class<? extends ShardsAllocator> clazz) throws IOException {
        while (cluster().size() != 0) {
            internalCluster().stopRandomDataNode();
        }
        internalCluster().startNode(settings);
        ShardsAllocator instance = internalCluster().getInstance(ShardsAllocator.class);
        assertThat(instance, instanceOf(clazz));
    }
}

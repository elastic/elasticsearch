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
package org.elasticsearch.common.settings;

import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 */
public class ClusterSettingsTests extends ESTestCase {

    public void testGet() {
        ClusterSettings settings = new ClusterSettings();
        Setting setting = settings.get("cluster.routing.allocation.require.value");
        assertEquals(setting, FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING);

        setting = settings.get("cluster.routing.allocation.total_shards_per_node");
        assertEquals(setting, ShardsLimitAllocationDecider.CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING);
    }

    public void testIsDynamic(){
        ClusterSettings settings = new ClusterSettings(new HashSet<>(Arrays.asList(Setting.intSetting("foo.bar", 1, true, Setting.Scope.Cluster), Setting.intSetting("foo.bar.baz", 1, false, Setting.Scope.Cluster))));
        assertFalse(settings.hasDynamicSetting("foo.bar.baz"));
        assertTrue(settings.hasDynamicSetting("foo.bar"));
        assertNotNull(settings.get("foo.bar.baz"));
    }

    public void testDiff() throws IOException {
        Setting<Integer> foobarbaz = Setting.intSetting("foo.bar.baz", 1, false, Setting.Scope.Cluster);
        Setting<Integer> foobar = Setting.intSetting("foo.bar", 1, true, Setting.Scope.Cluster);
        ClusterSettings settings = new ClusterSettings(new HashSet<>(Arrays.asList(foobar, foobarbaz)));
        Settings diff = settings.diff(Settings.builder().put("foo.bar", 5).build());
        assertEquals(diff.getAsMap().size(), 1);
        assertEquals(diff.getAsInt("foo.bar.baz", null), Integer.valueOf(1));
    }
}

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

package org.elasticsearch.cluster.routing.allocation.decider;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.DiskUsage;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Unit tests for the DiskThresholdDecider
 */
public class DiskThresholdDeciderUnitTests extends ElasticsearchTestCase {

    @Test
    public void testDynamicSettings() {
        NodeSettingsService nss = new NodeSettingsService(ImmutableSettings.EMPTY);

        ClusterInfoService cis = new ClusterInfoService() {
            @Override
            public ClusterInfo getClusterInfo() {
                Map<String, DiskUsage> usages = new HashMap<>();
                Map<String, Long> shardSizes = new HashMap<>();
                return new ClusterInfo(ImmutableMap.copyOf(usages), ImmutableMap.copyOf(shardSizes));
            }

            @Override
            public void addListener(Listener listener) {
                // noop
            }
        };
        DiskThresholdDecider decider = new DiskThresholdDecider(ImmutableSettings.EMPTY, nss, cis, null);

        assertThat(decider.getFreeBytesThresholdHigh(), equalTo(ByteSizeValue.parseBytesSizeValue("0b")));
        assertThat(decider.getFreeDiskThresholdHigh(), equalTo(10.0d));
        assertThat(decider.getFreeBytesThresholdLow(), equalTo(ByteSizeValue.parseBytesSizeValue("0b")));
        assertThat(decider.getFreeDiskThresholdLow(), equalTo(15.0d));
        assertThat(decider.getRerouteInterval().seconds(), equalTo(60L));
        assertTrue(decider.isEnabled());
        assertTrue(decider.isIncludeRelocations());

        DiskThresholdDecider.ApplySettings applySettings = decider.newApplySettings();

        Settings newSettings = ImmutableSettings.builder()
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED, false)
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS, false)
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK, "70%")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK, "500mb")
                .put(DiskThresholdDecider.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL, "30s")
                .build();

        applySettings.onRefreshSettings(newSettings);

        assertThat("high threshold bytes should be unset",
                decider.getFreeBytesThresholdHigh(), equalTo(ByteSizeValue.parseBytesSizeValue("0b")));
        assertThat("high threshold percentage should be changed",
                decider.getFreeDiskThresholdHigh(), equalTo(30.0d));
        assertThat("low threshold bytes should be set to 500mb",
                decider.getFreeBytesThresholdLow(), equalTo(ByteSizeValue.parseBytesSizeValue("500mb")));
        assertThat("low threshold bytes should be unset",
                decider.getFreeDiskThresholdLow(), equalTo(0.0d));
        assertThat("reroute interval should be changed to 30 seconds",
                decider.getRerouteInterval().seconds(), equalTo(30L));
        assertFalse("disk threshold decider should now be disabled", decider.isEnabled());
        assertFalse("relocations should now be disabled", decider.isIncludeRelocations());
    }

}

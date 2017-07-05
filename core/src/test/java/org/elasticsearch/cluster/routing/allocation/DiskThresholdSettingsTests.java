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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class DiskThresholdSettingsTests extends ESTestCase {

    public void testDefaults() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        ByteSizeValue zeroBytes = ByteSizeValue.parseBytesSizeValue("0b", "test");
        assertEquals(zeroBytes, diskThresholdSettings.getFreeBytesThresholdHigh());
        assertEquals(10.0D, diskThresholdSettings.getFreeDiskThresholdHigh(), 0.0D);
        assertEquals(zeroBytes, diskThresholdSettings.getFreeBytesThresholdLow());
        assertEquals(15.0D, diskThresholdSettings.getFreeDiskThresholdLow(), 0.0D);
        assertEquals(60L, diskThresholdSettings.getRerouteInterval().seconds());
        assertTrue(diskThresholdSettings.isEnabled());
        assertTrue(diskThresholdSettings.includeRelocations());
    }

    public void testUpdate() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "500mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s")
            .build();
        nss.applySettings(newSettings);

        assertEquals(ByteSizeValue.parseBytesSizeValue("0b", "test"), diskThresholdSettings.getFreeBytesThresholdHigh());
        assertEquals(30.0D, diskThresholdSettings.getFreeDiskThresholdHigh(), 0.0D);
        assertEquals(ByteSizeValue.parseBytesSizeValue("500mb", "test"), diskThresholdSettings.getFreeBytesThresholdLow());
        assertEquals(0.0D, diskThresholdSettings.getFreeDiskThresholdLow(), 0.0D);
        assertEquals(30L, diskThresholdSettings.getRerouteInterval().seconds());
        assertFalse(diskThresholdSettings.isEnabled());
        assertFalse(diskThresholdSettings.includeRelocations());
    }

    public void testInvalidUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "80%")
                .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [90%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("low disk watermark [90%] more than high disk watermark [80%]")));
    }

}

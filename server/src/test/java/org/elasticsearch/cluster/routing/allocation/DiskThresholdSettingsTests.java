/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Locale;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class DiskThresholdSettingsTests extends ESTestCase {

    public void testDefaults() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        assertEquals(60L, diskThresholdSettings.getRerouteInterval().seconds());
        assertTrue(diskThresholdSettings.isEnabled());

        // Test default watermark percentages
        ByteSizeValue hundredBytes = ByteSizeValue.parseBytesSizeValue("100b", "test");
        assertEquals(ByteSizeValue.ofBytes(15), diskThresholdSettings.getFreeBytesThresholdLowStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(10), diskThresholdSettings.getFreeBytesThresholdHighStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(5), diskThresholdSettings.getFreeBytesThresholdFloodStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(5), diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(hundredBytes));
        assertEquals("85%", diskThresholdSettings.describeLowThreshold(hundredBytes, false));
        assertEquals("90%", diskThresholdSettings.describeHighThreshold(hundredBytes, false));
        assertEquals("95%", diskThresholdSettings.describeFloodStageThreshold(hundredBytes, false));
        assertEquals("95%", diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, false));
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey() + "=" + "85%",
            diskThresholdSettings.describeLowThreshold(hundredBytes, true)
        );
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey() + "=" + "90%",
            diskThresholdSettings.describeHighThreshold(hundredBytes, true)
        );
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey() + "=" + "95%",
            diskThresholdSettings.describeFloodStageThreshold(hundredBytes, true)
        );
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey() + "=" + "95%",
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, true)
        );

        // Test default watermark max headroom values
        ByteSizeValue thousandTb = ByteSizeValue.parseBytesSizeValue("1000tb", "test");
        ByteSizeValue frozenFloodHeadroom = ByteSizeValue.parseBytesSizeValue("20gb", "test");
        assertEquals(frozenFloodHeadroom, diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(thousandTb));
        assertEquals("max_headroom=20gb", diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, false));
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey() + "=" + "20gb",
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, true)
        );
    }

    public void testMinimumTotalSizeForBelowLowWatermark() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        // Test default values

        // For 850 used bytes, we need 850 / 0.85 = 1000 total bytes.
        assertEquals(
            ByteSizeValue.ofBytes(1000),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );

        // Test random factor.
        final long factor = between(1, 1000);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(85 * factor)),
            Matchers.equalTo(ByteSizeValue.ofBytes(100L * factor))
        );

        // Test absolute values

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1gb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "100mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "10mb")
            .build();
        nss.applySettings(newSettings);

        // For 850 used bytes, we need 850b + 1GB total bytes.
        assertEquals(
            ByteSizeValue.ofBytes(ByteSizeValue.ofGb(1).getBytes() + ByteSizeValue.ofBytes(850).getBytes()),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );
        // For 100TB used bytes, we need 100TB+1GB total bytes.
        assertEquals(
            ByteSizeValue.ofBytes(ByteSizeValue.ofTb(100).getBytes() + ByteSizeValue.ofGb(1).getBytes()),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofTb(100))
        );

        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "0.50")
            .build();
        nss.applySettings(newSettings);

        // For 850 used bytes, we need 850 / 0.5 = 1700 total bytes
        assertEquals(
            ByteSizeValue.ofBytes(1700),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );
        // For 100TB used bytes, we need 100TB / 0.5 total bytes.
        assertEquals(
            ByteSizeValue.ofBytes((long) (ByteSizeValue.ofTb(100).getBytes() / 0.5)),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofTb(100))
        );

        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "0.50")
            .build();
        nss.applySettings(newSettings);

        // For 850 used bytes, we need 850 / 0.5 = 1700 total bytes
        assertEquals(
            ByteSizeValue.ofBytes(1700),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );

        // Test random percentage

        // to make it easy, stay below high watermark.
        final long percentage = between(1, 89);
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), percentage + "%")
            .build();
        nss.applySettings(newSettings);

        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(percentage * factor)),
            Matchers.equalTo(ByteSizeValue.ofBytes(100L * factor))
        );

        // Test case for 17777 used bytes & threshold 0.29. Should return 61300 bytes. Test case originates from issue #88791.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "29%")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(17777L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(61300))
        );

        // Test random absolute values

        final long absolute = between(1, 1000);
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), absolute + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), absolute + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), absolute + "b")
            .build();
        nss.applySettings(newSettings);

        long needed = between(0, 1000);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(needed)),
            Matchers.equalTo(ByteSizeValue.ofBytes(needed + absolute))
        );
    }

    public void testUpdateWatermarkByteValues() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1000mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "500mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "250mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "150mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s")
            .build();
        nss.applySettings(newSettings);

        ByteSizeValue total = ByteSizeValue.parseBytesSizeValue("1000tb", "test");
        assertEquals(ByteSizeValue.parseBytesSizeValue("1000mb", "test"), diskThresholdSettings.getFreeBytesThresholdLowStage(total));
        assertEquals(ByteSizeValue.parseBytesSizeValue("500mb", "test"), diskThresholdSettings.getFreeBytesThresholdHighStage(total));
        assertEquals(ByteSizeValue.parseBytesSizeValue("250mb", "test"), diskThresholdSettings.getFreeBytesThresholdFloodStage(total));
        assertEquals(
            ByteSizeValue.parseBytesSizeValue("150mb", "test"),
            diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(total)
        );
        assertEquals(30L, diskThresholdSettings.getRerouteInterval().seconds());
        assertFalse(diskThresholdSettings.isEnabled());
    }

    public void testUpdateWatermarkPercentageValues() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), randomBoolean() ? "50%" : "0.50")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), randomBoolean() ? "60%" : "0.60")
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                randomBoolean() ? "75%" : "0.75"
            )
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
                randomBoolean() ? "80%" : "0.80"
            )
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s")
            .build();
        nss.applySettings(newSettings);

        ByteSizeValue total = ByteSizeValue.parseBytesSizeValue("100b", "test");
        assertEquals(ByteSizeValue.parseBytesSizeValue("50b", "test"), diskThresholdSettings.getFreeBytesThresholdLowStage(total));
        assertEquals(ByteSizeValue.parseBytesSizeValue("40b", "test"), diskThresholdSettings.getFreeBytesThresholdHighStage(total));
        assertEquals(ByteSizeValue.parseBytesSizeValue("25b", "test"), diskThresholdSettings.getFreeBytesThresholdFloodStage(total));
        assertEquals(ByteSizeValue.parseBytesSizeValue("20b", "test"), diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(total));
        assertEquals(30L, diskThresholdSettings.getRerouteInterval().seconds());
        assertFalse(diskThresholdSettings.isEnabled());
    }

    public void testInvalidConstruction() {
        final Settings settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "80%")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new DiskThresholdSettings(settings, clusterSettings)
        );
        assertThat(e, hasToString(containsString("low disk watermark [90%] more than high disk watermark [80%]")));
    }

    public void testInvalidLowHighPercentageUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "83.45%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [90%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("low disk watermark [90%] more than high disk watermark [83.45%]")));
    }

    public void testInvalidHighFloodPercentageUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "50.1%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "55%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [50.1%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("high disk watermark [60%] more than flood stage disk watermark [55%]")));
    }

    public void testInvalidLowHighBytesUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "500m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1000m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "250m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [500m]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("low disk watermark [500mb] less than high disk watermark [1000mb]")));
    }

    public void testInvalidHighFloodBytesUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "500m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1000m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "750m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [500m]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("low disk watermark [500mb] less than high disk watermark [1000mb]")));
    }

    public void testIncompatibleThresholdUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1000m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "95.2%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low] from [85%] to [90%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String incompatibleExpected = String.format(
            Locale.ROOT,
            "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
            "90%",
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
            "1000mb",
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
            "95.2%"
        );
        assertThat(cause, hasToString(containsString(incompatibleExpected)));
    }

    public void testInvalidHighDiskThreshold() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "75%")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.high] from [90%] to [75%]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("low disk watermark [85%] more than high disk watermark [75%]")));
    }

    public void testSequenceOfUpdates() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings.Builder target = Settings.builder();

        {
            final Settings settings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "99%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "persistent"));
            assertNull(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()));
            assertNull(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()));
            assertThat(
                target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()),
                equalTo("99%")
            );
        }

        {
            final Settings settings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "97%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "persistent"));
            assertNull(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()));
            assertThat(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()), equalTo("97%"));
            assertThat(
                target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()),
                equalTo("99%")
            );
        }

        {
            final Settings settings = Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "95%")
                .build();
            final Settings.Builder updates = Settings.builder();
            assertTrue(clusterSettings.updateSettings(settings, target, updates, "persistent"));
            assertThat(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()), equalTo("95%"));
            assertThat(target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()), equalTo("97%"));
            assertThat(
                target.get(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()),
                equalTo("99%")
            );
        }
    }

    private void doTestDescriptions(boolean includeKey) {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ByteSizeValue hundredBytes = ByteSizeValue.parseBytesSizeValue("100b", "test");
        ByteSizeValue thousandTb = ByteSizeValue.parseBytesSizeValue("1000tb", "test");
        String lowWatermarkPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey() + "="
            : "";
        String highWatermarkPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey() + "="
            : "";
        String floodWatermarkPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey() + "="
            : "";
        String frozenFloodWatermarkPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey() + "="
            : "";
        String frozenFloodMaxHeadroomPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey() + "="
            : "max_headroom=";

        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, clusterSettings);
        assertThat(diskThresholdSettings.describeLowThreshold(hundredBytes, includeKey), equalTo(lowWatermarkPrefix + "85%"));
        assertThat(diskThresholdSettings.describeHighThreshold(hundredBytes, includeKey), equalTo(highWatermarkPrefix + "90%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(hundredBytes, includeKey), equalTo(floodWatermarkPrefix + "95%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "95%")
        );

        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, includeKey),
            equalTo(frozenFloodMaxHeadroomPrefix + "20gb")
        );

        diskThresholdSettings = new DiskThresholdSettings(
            Settings.builder()
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "91.2%" : "0.912"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "91.3%" : "0.913"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "91.4%" : "0.914"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "91.5%" : "0.915"
                )
                .build(),
            clusterSettings
        );

        assertThat(diskThresholdSettings.describeLowThreshold(hundredBytes, includeKey), equalTo(lowWatermarkPrefix + "91.2%"));
        assertThat(diskThresholdSettings.describeHighThreshold(hundredBytes, includeKey), equalTo(highWatermarkPrefix + "91.3%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(hundredBytes, includeKey), equalTo(floodWatermarkPrefix + "91.4%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "91.5%")
        );

        // Even for 1000TB, the watermarks apply since they are set (any max headroom does not apply)
        assertThat(diskThresholdSettings.describeLowThreshold(thousandTb, includeKey), equalTo(lowWatermarkPrefix + "91.2%"));
        assertThat(diskThresholdSettings.describeHighThreshold(thousandTb, includeKey), equalTo(highWatermarkPrefix + "91.3%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(thousandTb, includeKey), equalTo(floodWatermarkPrefix + "91.4%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "91.5%")
        );

        diskThresholdSettings = new DiskThresholdSettings(
            Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1GB")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10MB")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "2B")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "1B")
                // Max headroom values should be ignored since the watermark values are set to absolute values
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "10mb")
                .build(),
            clusterSettings
        );

        assertThat(diskThresholdSettings.describeLowThreshold(hundredBytes, includeKey), equalTo(lowWatermarkPrefix + "1gb"));
        assertThat(diskThresholdSettings.describeHighThreshold(hundredBytes, includeKey), equalTo(highWatermarkPrefix + "10mb"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(hundredBytes, includeKey), equalTo(floodWatermarkPrefix + "2b"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "1b")
        );

        // Even for 1000TB, the watermarks apply since they are set to absolute values (max headroom values should be ignored)
        assertThat(diskThresholdSettings.describeLowThreshold(thousandTb, includeKey), equalTo(lowWatermarkPrefix + "1gb"));
        assertThat(diskThresholdSettings.describeHighThreshold(thousandTb, includeKey), equalTo(highWatermarkPrefix + "10mb"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(thousandTb, includeKey), equalTo(floodWatermarkPrefix + "2b"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "1b")
        );

        // Test a mixture of percentages and max headroom values
        diskThresholdSettings = new DiskThresholdSettings(
            Settings.builder()
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "31.2%" : "0.312"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "31.3%" : "0.313"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "31.4%" : "0.314"
                )
                .put(
                    DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
                    randomBoolean() ? "31.5%" : "0.315"
                )
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "10gb")
                .build(),
            clusterSettings
        );

        assertThat(diskThresholdSettings.describeLowThreshold(hundredBytes, includeKey), equalTo(lowWatermarkPrefix + "31.2%"));
        assertThat(diskThresholdSettings.describeHighThreshold(hundredBytes, includeKey), equalTo(highWatermarkPrefix + "31.3%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(hundredBytes, includeKey), equalTo(floodWatermarkPrefix + "31.4%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "31.5%")
        );

        assertThat(diskThresholdSettings.describeLowThreshold(thousandTb, includeKey), equalTo(lowWatermarkPrefix + "31.2%"));
        assertThat(diskThresholdSettings.describeHighThreshold(thousandTb, includeKey), equalTo(highWatermarkPrefix + "31.3%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(thousandTb, includeKey), equalTo(floodWatermarkPrefix + "31.4%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, includeKey),
            equalTo(frozenFloodMaxHeadroomPrefix + "10gb")
        );
    }

    public void testDescriptionsWithKeys() {
        doTestDescriptions(true);
    }

    public void testDescriptionsWithoutKeys() {
        doTestDescriptions(false);
    }

}

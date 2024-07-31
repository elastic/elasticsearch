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
        ByteSizeValue lowHeadroom = ByteSizeValue.parseBytesSizeValue("200gb", "test");
        ByteSizeValue highHeadroom = ByteSizeValue.parseBytesSizeValue("150gb", "test");
        ByteSizeValue floodHeadroom = ByteSizeValue.parseBytesSizeValue("100gb", "test");
        ByteSizeValue frozenFloodHeadroom = ByteSizeValue.parseBytesSizeValue("20gb", "test");
        assertEquals(lowHeadroom, diskThresholdSettings.getFreeBytesThresholdLowStage(thousandTb));
        assertEquals(highHeadroom, diskThresholdSettings.getFreeBytesThresholdHighStage(thousandTb));
        assertEquals(floodHeadroom, diskThresholdSettings.getFreeBytesThresholdFloodStage(thousandTb));
        assertEquals(frozenFloodHeadroom, diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(thousandTb));
        assertEquals("max_headroom=200gb", diskThresholdSettings.describeLowThreshold(thousandTb, false));
        assertEquals("max_headroom=150gb", diskThresholdSettings.describeHighThreshold(thousandTb, false));
        assertEquals("max_headroom=100gb", diskThresholdSettings.describeFloodStageThreshold(thousandTb, false));
        assertEquals("max_headroom=20gb", diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, false));
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey() + "=" + "200gb",
            diskThresholdSettings.describeLowThreshold(thousandTb, true)
        );
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey() + "=" + "150gb",
            diskThresholdSettings.describeHighThreshold(thousandTb, true)
        );
        assertEquals(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey() + "=" + "100gb",
            diskThresholdSettings.describeFloodStageThreshold(thousandTb, true)
        );
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
        // For 100TB used bytes, the max headroom should cap the minimum required free space to 200GB. So we need 100TB+200GB total bytes.
        assertEquals(
            ByteSizeValue.add(ByteSizeValue.ofTb(100), ByteSizeValue.ofGb(200)),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofTb(100))
        );

        // Test random factor. Stay in low values so max headroom does not apply.
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
            ByteSizeValue.add(ByteSizeValue.ofGb(1), ByteSizeValue.ofBytes(850)),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );
        // For 100TB used bytes, we need 100TB+1GB total bytes.
        assertEquals(
            ByteSizeValue.add(ByteSizeValue.ofTb(100), ByteSizeValue.ofGb(1)),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofTb(100))
        );

        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "0.50")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "-1")
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
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "500gb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "150gb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "100gb")
            .build();
        nss.applySettings(newSettings);

        // For 850 used bytes, we need 850 / 0.5 = 1700 total bytes
        assertEquals(
            ByteSizeValue.ofBytes(1700),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(850))
        );
        // For 100TB used bytes, the max headroom should cap the minimum required free space to 500GB. So we need 100TB+500GB total bytes.
        assertEquals(
            ByteSizeValue.add(ByteSizeValue.ofTb(100), ByteSizeValue.ofGb(500)),
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofTb(100))
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

        // Test case for 32547 used bytes & threshold 0.57. Should return 57100 bytes.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "57%")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(32547L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(57100L))
        );

        // Test case for 4080 used bytes & threshold 0.68. Should return 6000 bytes.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "68%")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(4080L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(6000))
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

        // Test case for 90 used bytes & threshold 0.90. Should return 100 bytes.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(90L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(100L))
        );

        // Test case for 90 used bytes & threshold 0.90 & max headroom of 1 byte. Should return 91 bytes.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "1b")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(90L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(91L))
        );

        // Test case for 90 used bytes & threshold 0.90 & max headroom of 0 bytes. Should return 90 bytes.
        newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "0b")
            .build();
        nss.applySettings(newSettings);
        assertThat(
            diskThresholdSettings.getMinimumTotalSizeForBelowLowWatermark(ByteSizeValue.ofBytes(90L)),
            Matchers.equalTo(ByteSizeValue.ofBytes(90L))
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

    public void testUpdateMaxHeadroomValues() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "1000mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "500mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "250mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "150mb")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s")
            .build();
        nss.applySettings(newSettings);

        // Test that default percentage values apply
        ByteSizeValue hundredBytes = ByteSizeValue.parseBytesSizeValue("100b", "test");
        assertEquals(ByteSizeValue.ofBytes(15), diskThresholdSettings.getFreeBytesThresholdLowStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(10), diskThresholdSettings.getFreeBytesThresholdHighStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(5), diskThresholdSettings.getFreeBytesThresholdFloodStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(5), diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(hundredBytes));

        // Test that max headroom values apply
        ByteSizeValue thousandTb = ByteSizeValue.parseBytesSizeValue("1000tb", "test");
        ByteSizeValue lowHeadroom = ByteSizeValue.parseBytesSizeValue("1000mb", "test");
        ByteSizeValue highHeadroom = ByteSizeValue.parseBytesSizeValue("500mb", "test");
        ByteSizeValue floodHeadroom = ByteSizeValue.parseBytesSizeValue("250mb", "test");
        ByteSizeValue frozenFloodHeadroom = ByteSizeValue.parseBytesSizeValue("150mb", "test");
        assertEquals(lowHeadroom, diskThresholdSettings.getFreeBytesThresholdLowStage(thousandTb));
        assertEquals(highHeadroom, diskThresholdSettings.getFreeBytesThresholdHighStage(thousandTb));
        assertEquals(floodHeadroom, diskThresholdSettings.getFreeBytesThresholdFloodStage(thousandTb));
        assertEquals(frozenFloodHeadroom, diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(thousandTb));
    }

    public void testUpdateMaxHeadroomValuesLowValues() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s")
            .build();
        nss.applySettings(newSettings);

        // Test that the max headroom values prevail over default watermark ratios
        ByteSizeValue hundredBytes = ByteSizeValue.parseBytesSizeValue("100b", "test");
        assertEquals(ByteSizeValue.ONE, diskThresholdSettings.getFreeBytesThresholdLowStage(hundredBytes));
        assertEquals(ByteSizeValue.ZERO, diskThresholdSettings.getFreeBytesThresholdHighStage(hundredBytes));
        assertEquals(ByteSizeValue.ZERO, diskThresholdSettings.getFreeBytesThresholdFloodStage(hundredBytes));
        assertEquals(ByteSizeValue.ZERO, diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(hundredBytes));
    }

    public void testUpdateWatermarkAndMaxHeadroomValues() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        boolean watermarksAbsolute = randomBoolean();
        boolean lowHeadroomEnabled = (watermarksAbsolute == false) && randomBoolean();
        boolean highHeadroomEnabled = lowHeadroomEnabled ? true : ((watermarksAbsolute == false) && randomBoolean());
        boolean floodHeadroomEnabled = highHeadroomEnabled ? true : ((watermarksAbsolute == false) && randomBoolean());
        boolean frozenFloodHeadroomEnabled = (watermarksAbsolute == false) && randomBoolean();

        Settings.Builder builder = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                watermarksAbsolute ? "50b" : randomBoolean() ? "50%" : "0.50"
            )
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                watermarksAbsolute ? "40b" : randomBoolean() ? "60%" : "0.60"
            )
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                watermarksAbsolute ? "30b" : randomBoolean() ? "70%" : "0.70"
            )
            .put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
                watermarksAbsolute ? "15b" : randomBoolean() ? "85%" : "0.85"
            )
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "30s");
        if (lowHeadroomEnabled) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "1000mb");
        }
        if (highHeadroomEnabled) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "500mb");
        }
        if (floodHeadroomEnabled) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "250mb");
        }
        if (frozenFloodHeadroomEnabled) {
            builder = builder.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(),
                "150mb"
            );
        }
        nss.applySettings(builder.build());

        // Test that watermark values apply
        ByteSizeValue hundredBytes = ByteSizeValue.parseBytesSizeValue("100b", "test");
        assertEquals(ByteSizeValue.ofBytes(50), diskThresholdSettings.getFreeBytesThresholdLowStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(40), diskThresholdSettings.getFreeBytesThresholdHighStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(30), diskThresholdSettings.getFreeBytesThresholdFloodStage(hundredBytes));
        assertEquals(ByteSizeValue.ofBytes(15), diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(hundredBytes));

        // Test that max headroom values (if enabled) prevail over percentage watermark values
        ByteSizeValue thousandTb = ByteSizeValue.parseBytesSizeValue("1000tb", "test");
        ByteSizeValue lowExpected = ByteSizeValue.parseBytesSizeValue(
            watermarksAbsolute ? "50b" : lowHeadroomEnabled ? "1000mb" : "500tb",
            "test"
        );
        ByteSizeValue highExpected = ByteSizeValue.parseBytesSizeValue(
            watermarksAbsolute ? "40b" : highHeadroomEnabled ? "500mb" : "400tb",
            "test"
        );
        ByteSizeValue floodExpected = ByteSizeValue.parseBytesSizeValue(
            watermarksAbsolute ? "30b" : floodHeadroomEnabled ? "250mb" : "300tb",
            "test"
        );
        ByteSizeValue frozenFloodExpected = ByteSizeValue.parseBytesSizeValue(
            watermarksAbsolute ? "15b" : frozenFloodHeadroomEnabled ? "150mb" : "150tb",
            "test"
        );
        assertEquals(lowExpected, diskThresholdSettings.getFreeBytesThresholdLowStage(thousandTb));
        assertEquals(highExpected, diskThresholdSettings.getFreeBytesThresholdHighStage(thousandTb));
        assertEquals(floodExpected, diskThresholdSettings.getFreeBytesThresholdFloodStage(thousandTb));
        assertEquals(frozenFloodExpected, diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(thousandTb));
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

    public void testIncompatibleMaxHeadroomUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        Settings.Builder settings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "300g")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "200g")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "100g");

        String lowHeadroom = "-1";
        String highHeadroom = "-1";
        String floodHeadroom = "-1";
        if (randomBoolean()) {
            lowHeadroom = "100gb";
            settings = settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), lowHeadroom);
        } else if (randomBoolean()) {
            highHeadroom = "100gb";
            settings = settings.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), highHeadroom);
        } else {
            floodHeadroom = "100gb";
            settings = settings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(),
                floodHeadroom
            );
        }
        final Settings builtSettings = settings.build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(builtSettings));
        final String expected = "illegal value can't update [cluster.routing.allocation.disk.watermark.low.max_headroom] from [200GB] to ["
            + lowHeadroom
            + "]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String incompatibleExpected = String.format(
            Locale.ROOT,
            "At least one of the disk max headroom settings is set [low=%s, high=%s, flood=%s], while the disk watermark values "
                + "are set to absolute values instead of ratios/percentages, e.g., the low watermark is [%s]",
            lowHeadroom,
            highHeadroom,
            floodHeadroom,
            "300gb"
        );
        assertThat(cause, hasToString(containsString(incompatibleExpected)));
    }

    public void testIncompatibleFrozenMaxHeadroomUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "300g")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey(), "100g")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected =
            "illegal value can't update [cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom] from [20GB] to [100g]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        final String incompatibleExpected = String.format(
            Locale.ROOT,
            "The frozen flood stage disk max headroom setting is set [%s], while the frozen flood stage disk watermark setting "
                + "is set to an absolute value instead of a ratio/percentage [%s]",
            "100gb",
            "300gb"
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

    public void testMaxHeadroomDefaultWhenWatermarkIsSet() {
        ClusterSettings nss = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, nss);

        Settings.Builder newSettings = Settings.builder();
        if (randomBoolean()) {
            newSettings = newSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getDefault(Settings.EMPTY).getStringRep()
            );
        } else if (randomBoolean()) {
            newSettings = newSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getDefault(Settings.EMPTY).getStringRep()
            );
        } else {
            newSettings = newSettings.put(
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getDefault(Settings.EMPTY)
                    .getStringRep()
            );
        }
        newSettings = newSettings.put(
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(),
            DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getDefault(Settings.EMPTY)
                .getStringRep()
        );
        nss.applySettings(newSettings.build());

        // Test that max headroom values do not apply
        assertEquals(ByteSizeValue.ofTb(150), diskThresholdSettings.getFreeBytesThresholdLowStage(ByteSizeValue.ofTb(1000)));
        assertEquals(ByteSizeValue.ofTb(100), diskThresholdSettings.getFreeBytesThresholdHighStage(ByteSizeValue.ofTb(1000)));
        assertEquals(ByteSizeValue.ofTb(50), diskThresholdSettings.getFreeBytesThresholdFloodStage(ByteSizeValue.ofTb(1000)));
        assertEquals(ByteSizeValue.ofTb(50), diskThresholdSettings.getFreeBytesThresholdFrozenFloodStage(ByteSizeValue.ofTb(1000)));
    }

    public void testInvalidLowHighMaxHeadroomUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "300m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "750m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "500m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected =
            "illegal value can't update [cluster.routing.allocation.disk.watermark.low.max_headroom] from [200GB] to [300m]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("high disk max headroom [750mb] more than low disk max headroom [300mb]")));
    }

    public void testInvalidHighFloodMaxHeadroomUpdate() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings); // this has the effect of registering the settings updater

        final Settings newSettings = Settings.builder()
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "400m")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "500m")
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected =
            "illegal value can't update [cluster.routing.allocation.disk.watermark.high.max_headroom] from [150GB] to [400m]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("flood disk max headroom [500mb] more than high disk max headroom [400mb]")));
    }

    public void testInvalidHeadroomSetToMinusOne() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings);

        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "-1");
        } else if (randomBoolean()) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "-1");
        } else {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "-1");
        }
        final Settings newSettings = builder.build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(cause, hasToString(containsString("setting a headroom value to less than 0 is not supported")));
    }

    public void testInvalidLowHeadroomSetAndHighNotSet() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings);

        Settings.Builder builder = Settings.builder()
            // The following settings combination for a 1000TiB hard disk would result in the required minimum free disk space for the low
            // watermark to be 150GiB, and for the high 100TiB. So it could hit the high watermark before the low watermark.
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "85%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "95%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "150GB");
        if (randomBoolean()) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "20GB");
        }
        final Settings newSettings = builder.build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected =
            "illegal value can't update [cluster.routing.allocation.disk.watermark.low.max_headroom] from [200GB] to [150GB]";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(
            cause,
            hasToString(containsString("high disk max headroom [-1] is not set, while the low disk max headroom is set [150gb]"))
        );
    }

    public void testInvalidHighHeadroomSetAndFloodNotSet() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        new DiskThresholdSettings(Settings.EMPTY, clusterSettings);

        Settings.Builder builder = Settings.builder()
            // The following settings combination for a 1000TiB hard disk would result in the required minimum free disk space for the high
            // watermark to be 150GiB and for the flood 50TiB. So it could hit the flood watermark before the high watermark.
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "85%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "90%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "95%")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "150GB");
        if (randomBoolean()) {
            builder = builder.put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "200GB");
        }
        final Settings newSettings = builder.build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(newSettings));
        final String expected = "illegal value can't update";
        assertThat(e, hasToString(containsString(expected)));
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        final IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
        assertThat(
            cause,
            hasToString(containsString("flood disk max headroom [-1] is not set, while the high disk max headroom is set [150gb]"))
        );
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
        String lowMaxHeadroomPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey() + "="
            : "max_headroom=";
        String highMaxHeadroomPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey() + "="
            : "max_headroom=";
        String floodMaxHeadroomPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey() + "="
            : "max_headroom=";
        String frozenFloodMaxHeadroomPrefix = includeKey
            ? DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.getKey() + "="
            : "max_headroom=";

        // Test default settings for watermarks

        DiskThresholdSettings diskThresholdSettings = new DiskThresholdSettings(Settings.EMPTY, clusterSettings);
        assertThat(diskThresholdSettings.describeLowThreshold(hundredBytes, includeKey), equalTo(lowWatermarkPrefix + "85%"));
        assertThat(diskThresholdSettings.describeHighThreshold(hundredBytes, includeKey), equalTo(highWatermarkPrefix + "90%"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(hundredBytes, includeKey), equalTo(floodWatermarkPrefix + "95%"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(hundredBytes, includeKey),
            equalTo(frozenFloodWatermarkPrefix + "95%")
        );

        assertThat(diskThresholdSettings.describeLowThreshold(thousandTb, includeKey), equalTo(lowMaxHeadroomPrefix + "200gb"));
        assertThat(diskThresholdSettings.describeHighThreshold(thousandTb, includeKey), equalTo(highMaxHeadroomPrefix + "150gb"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(thousandTb, includeKey), equalTo(floodMaxHeadroomPrefix + "100gb"));
        assertThat(
            diskThresholdSettings.describeFrozenFloodStageThreshold(thousandTb, includeKey),
            equalTo(frozenFloodMaxHeadroomPrefix + "20gb")
        );

        // Test a mixture of percentages without max headroom values

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

        // Test absolute values

        diskThresholdSettings = new DiskThresholdSettings(
            Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1GB")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "10MB")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "2B")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "1B")
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

        // Even for 1000TB, the watermarks apply since they are set (any max headroom does not apply)
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
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "100gb")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "50gb")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "10gb")
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

        assertThat(diskThresholdSettings.describeLowThreshold(thousandTb, includeKey), equalTo(lowMaxHeadroomPrefix + "100gb"));
        assertThat(diskThresholdSettings.describeHighThreshold(thousandTb, includeKey), equalTo(highMaxHeadroomPrefix + "50gb"));
        assertThat(diskThresholdSettings.describeFloodStageThreshold(thousandTb, includeKey), equalTo(floodMaxHeadroomPrefix + "10gb"));
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

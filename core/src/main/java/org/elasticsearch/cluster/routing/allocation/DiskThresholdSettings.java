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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A container to keep settings for disk thresholds up to date with cluster setting changes.
 */
public class DiskThresholdSettings {
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.threshold_enabled", true,
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.low", "85%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.low"),
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING =
        new Setting<>("cluster.routing.allocation.disk.watermark.high", "90%",
            (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.high"),
            Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING =
        Setting.boolSetting("cluster.routing.allocation.disk.include_relocations", true,
            Setting.Property.Dynamic, Setting.Property.NodeScope);;
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING =
        Setting.positiveTimeSetting("cluster.routing.allocation.disk.reroute_interval", TimeValue.timeValueSeconds(60),
            Setting.Property.Dynamic, Setting.Property.NodeScope);

    private volatile String lowWatermarkRaw;
    private volatile String highWatermarkRaw;
    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean includeRelocations;
    private volatile boolean enabled;
    private volatile TimeValue rerouteInterval;

    public DiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String lowWatermark = CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings);
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setLowWatermark(lowWatermark);
        this.includeRelocations = CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING.get(settings);
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_INCLUDE_RELOCATIONS_SETTING, this::setIncludeRelocations);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
    }

    private void setIncludeRelocations(boolean includeRelocations) {
        this.includeRelocations = includeRelocations;
    }

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setLowWatermark(String lowWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.lowWatermarkRaw = lowWatermark;
        this.freeDiskThresholdLow = 100.0 - thresholdPercentageFromWatermark(lowWatermark);
        this.freeBytesThresholdLow = thresholdBytesFromWatermark(lowWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey());
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.highWatermarkRaw = highWatermark;
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(highWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey());
    }

    /**
     * Gets the raw (uninterpreted) low watermark value as found in the settings.
     */
    public String getLowWatermarkRaw() {
        return lowWatermarkRaw;
    }

    /**
     * Gets the raw (uninterpreted) high watermark value as found in the settings.
     */
    public String getHighWatermarkRaw() {
        return highWatermarkRaw;
    }

    public Double getFreeDiskThresholdLow() {
        return freeDiskThresholdLow;
    }

    public Double getFreeDiskThresholdHigh() {
        return freeDiskThresholdHigh;
    }

    public ByteSizeValue getFreeBytesThresholdLow() {
        return freeBytesThresholdLow;
    }

    public ByteSizeValue getFreeBytesThresholdHigh() {
        return freeBytesThresholdHigh;
    }

    public boolean includeRelocations() {
        return includeRelocations;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    private double thresholdPercentageFromWatermark(String watermark) {
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            return 100.0;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    private ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            return ByteSizeValue.parseBytesSizeValue("0b", settingName);
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * @return the watermark value given
     */
    private static String validWatermarkSetting(String watermark, String settingName) {
        try {
            RatioValue.parseRatioValue(watermark);
        } catch (ElasticsearchParseException e) {
            try {
                ByteSizeValue.parseBytesSizeValue(watermark, settingName);
            } catch (ElasticsearchParseException ex) {
                ex.addSuppressed(e);
                throw ex;
            }
        }
        return watermark;
    }
}

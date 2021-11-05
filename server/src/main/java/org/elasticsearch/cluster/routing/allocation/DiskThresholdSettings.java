/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * A container to keep settings for disk thresholds up to date with cluster setting changes.
 */
public class DiskThresholdSettings {
    public static final Setting<Boolean> CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.disk.threshold_enabled",
        true,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.low",
        "85%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.low"),
        new LowDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.high",
        "90%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.high"),
        new HighDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage",
        "95%",
        (s) -> validWatermarkSetting(s, "cluster.routing.allocation.disk.watermark.flood_stage"),
        new FloodStageValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage.frozen",
        "95%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage.frozen"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom",
        (settings) -> {
            if (CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING.exists(settings)) {
                return "-1";
            } else {
                return "20GB";
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "cluster.routing.allocation.disk.watermark.flood_stage.frozen.max_headroom"),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "cluster.routing.allocation.disk.reroute_interval",
        TimeValue.timeValueSeconds(60),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile String lowWatermarkRaw;
    private volatile String highWatermarkRaw;
    private volatile Double freeDiskThresholdLow;
    private volatile Double freeDiskThresholdHigh;
    private volatile ByteSizeValue freeBytesThresholdLow;
    private volatile ByteSizeValue freeBytesThresholdHigh;
    private volatile boolean enabled;
    private volatile TimeValue rerouteInterval;
    private volatile Double freeDiskThresholdFloodStage;
    private volatile ByteSizeValue freeBytesThresholdFloodStage;
    private volatile RelativeByteSizeValue frozenFloodStage;
    private volatile ByteSizeValue frozenFloodStageMaxHeadroom;

    static {
        assert Version.CURRENT.major == Version.V_7_0_0.major + 1; // this check is unnecessary in v9
        final String AUTO_RELEASE_INDEX_ENABLED_KEY = "es.disk.auto_release_flood_stage_block";
        final String property = System.getProperty(AUTO_RELEASE_INDEX_ENABLED_KEY);
        if (property != null) {
            throw new IllegalArgumentException("system property [" + AUTO_RELEASE_INDEX_ENABLED_KEY + "] may not be set");
        }
    }

    public DiskThresholdSettings(Settings settings, ClusterSettings clusterSettings) {
        final String lowWatermark = CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.get(settings);
        final String highWatermark = CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.get(settings);
        final String floodStage = CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.get(settings);
        setHighWatermark(highWatermark);
        setLowWatermark(lowWatermark);
        setFloodStage(floodStage);
        setFrozenFloodStage(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING.get(settings));
        setFrozenFloodStageMaxHeadroom(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING.get(settings));
        this.rerouteInterval = CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.get(settings);
        this.enabled = CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING, this::setLowWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING, this::setHighWatermark);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING, this::setFloodStage);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_SETTING, this::setFrozenFloodStage);
        clusterSettings.addSettingsUpdateConsumer(
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_MAX_HEADROOM_SETTING,
            this::setFrozenFloodStageMaxHeadroom
        );
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING, this::setRerouteInterval);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING, this::setEnabled);
    }

    static final class LowDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(value, highWatermarkRaw, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    static final class HighDiskWatermarkValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String floodStageRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, value, floodStageRaw);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    static final class FloodStageValidator implements Setting.Validator<String> {

        @Override
        public void validate(final String value) {

        }

        @Override
        public void validate(final String value, final Map<Setting<?>, Object> settings) {
            final String lowWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING);
            final String highWatermarkRaw = (String) settings.get(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING);
            doValidate(lowWatermarkRaw, highWatermarkRaw, value);
        }

        @Override
        public Iterator<Setting<?>> settings() {
            final List<Setting<?>> settings = List.of(
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING
            );
            return settings.iterator();
        }

    }

    private static void doValidate(String low, String high, String flood) {
        if (definitelyNotPercentage(low) == false) { // only try to validate as percentage if it isn't obviously a byte size value
            try {
                doValidateAsPercentage(low, high, flood);
                return; // early return so that we do not try to parse as bytes
            } catch (final ElasticsearchParseException e) {
                // swallow as we are now going to try to parse as bytes
            }
        }
        try {
            doValidateAsBytes(low, high, flood);
        } catch (final ElasticsearchParseException e) {
            final String message = String.format(
                Locale.ROOT,
                "unable to consistently parse [%s=%s], [%s=%s], and [%s=%s] as percentage or bytes",
                CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                low,
                CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                high,
                CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                flood
            );
            throw new IllegalArgumentException(message, e);
        }
    }

    private static void doValidateAsPercentage(final String low, final String high, final String flood) {
        final double lowWatermarkThreshold = thresholdPercentageFromWatermark(low, false);
        final double highWatermarkThreshold = thresholdPercentageFromWatermark(high, false);
        final double floodThreshold = thresholdPercentageFromWatermark(flood, false);
        if (lowWatermarkThreshold > highWatermarkThreshold) {
            throw new IllegalArgumentException("low disk watermark [" + low + "] more than high disk watermark [" + high + "]");
        }
        if (highWatermarkThreshold > floodThreshold) {
            throw new IllegalArgumentException("high disk watermark [" + high + "] more than flood stage disk watermark [" + flood + "]");
        }
    }

    private static void doValidateAsBytes(final String low, final String high, final String flood) {
        final ByteSizeValue lowWatermarkBytes = thresholdBytesFromWatermark(
            low,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
            false
        );
        final ByteSizeValue highWatermarkBytes = thresholdBytesFromWatermark(
            high,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
            false
        );
        final ByteSizeValue floodStageBytes = thresholdBytesFromWatermark(
            flood,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
            false
        );
        if (lowWatermarkBytes.getBytes() < highWatermarkBytes.getBytes()) {
            throw new IllegalArgumentException("low disk watermark [" + low + "] less than high disk watermark [" + high + "]");
        }
        if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
            throw new IllegalArgumentException("high disk watermark [" + high + "] less than flood stage disk watermark [" + flood + "]");
        }
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
        this.freeBytesThresholdLow = thresholdBytesFromWatermark(
            lowWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()
        );
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.highWatermarkRaw = highWatermark;
        this.freeDiskThresholdHigh = 100.0 - thresholdPercentageFromWatermark(highWatermark);
        this.freeBytesThresholdHigh = thresholdBytesFromWatermark(
            highWatermark,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()
        );
    }

    private void setFloodStage(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdFloodStage = 100.0 - thresholdPercentageFromWatermark(floodStageRaw);
        this.freeBytesThresholdFloodStage = thresholdBytesFromWatermark(
            floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()
        );
    }

    private void setFrozenFloodStage(RelativeByteSizeValue floodStage) {
        this.frozenFloodStage = floodStage;
    }

    private void setFrozenFloodStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.frozenFloodStageMaxHeadroom = maxHeadroom;
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

    public Double getFreeDiskThresholdFloodStage() {
        return freeDiskThresholdFloodStage;
    }

    public ByteSizeValue getFreeBytesThresholdFloodStage() {
        return freeBytesThresholdFloodStage;
    }

    public ByteSizeValue getFreeBytesThresholdFrozenFloodStage(ByteSizeValue total) {
        // flood stage bytes are reversed compared to percentage, so we special handle it.
        RelativeByteSizeValue frozenFloodStage = this.frozenFloodStage;
        if (frozenFloodStage.isAbsolute()) {
            return frozenFloodStage.getAbsolute();
        }
        return ByteSizeValue.ofBytes(total.getBytes() - frozenFloodStage.calculateValue(total, frozenFloodStageMaxHeadroom).getBytes());
    }

    public boolean isEnabled() {
        return enabled;
    }

    public TimeValue getRerouteInterval() {
        return rerouteInterval;
    }

    String describeLowThreshold() {
        return freeBytesThresholdLow.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdLow, "%")
            : freeBytesThresholdLow.toString();
    }

    String describeHighThreshold() {
        return freeBytesThresholdHigh.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdHigh, "%")
            : freeBytesThresholdHigh.toString();
    }

    String describeFloodStageThreshold() {
        return freeBytesThresholdFloodStage.equals(ByteSizeValue.ZERO)
            ? Strings.format1Decimals(100.0 - freeDiskThresholdFloodStage, "%")
            : freeBytesThresholdFloodStage.toString();
    }

    String describeFrozenFloodStageThreshold(ByteSizeValue total) {
        ByteSizeValue maxHeadroom = this.frozenFloodStageMaxHeadroom;
        RelativeByteSizeValue floodStage = this.frozenFloodStage;
        if (floodStage.isAbsolute()) {
            return floodStage.getStringRep();
        } else if (floodStage.calculateValue(total, maxHeadroom).equals(floodStage.calculateValue(total, null))) {
            return Strings.format1Decimals(floodStage.getRatio().getAsPercent(), "%");
        } else {
            return "max_headroom=" + maxHeadroom;
        }
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if
     * it cannot be parsed.
     */
    private static double thresholdPercentageFromWatermark(String watermark) {
        return thresholdPercentageFromWatermark(watermark, true);
    }

    /**
     * Attempts to parse the watermark into a percentage, returning 100.0% if it can not be parsed and the specified lenient parameter is
     * true, otherwise throwing an {@link ElasticsearchParseException}.
     *
     * @param watermark the watermark to parse as a percentage
     * @param lenient true if lenient parsing should be applied
     * @return the parsed percentage
     */
    private static double thresholdPercentageFromWatermark(String watermark, boolean lenient) {
        if (lenient && definitelyNotPercentage(watermark)) {
            // obviously not a percentage so return lenient fallback value like we would below on a parse failure
            return 100.0;
        }
        try {
            return RatioValue.parseRatioValue(watermark).getAsPercent();
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return 100.0;
            }
            throw ex;
        }
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning
     * a ByteSizeValue of 0 bytes if the value cannot be parsed.
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName) {
        return thresholdBytesFromWatermark(watermark, settingName, true);
    }

    /**
     * Attempts to parse the watermark into a {@link ByteSizeValue}, returning zero bytes if it can not be parsed and the specified lenient
     * parameter is true, otherwise throwing an {@link ElasticsearchParseException}.
     *
     * @param watermark the watermark to parse as a byte size
     * @param settingName the name of the setting
     * @param lenient true if lenient parsing should be applied
     * @return the parsed byte size value
     */
    private static ByteSizeValue thresholdBytesFromWatermark(String watermark, String settingName, boolean lenient) {
        try {
            return ByteSizeValue.parseBytesSizeValue(watermark, settingName);
        } catch (ElasticsearchParseException ex) {
            // NOTE: this is not end-user leniency, since up above we check that it's a valid byte or percentage, and then store the two
            // cases separately
            if (lenient) {
                return ByteSizeValue.ZERO;
            }
            throw ex;
        }
    }

    /**
     * Checks if a watermark string is a valid percentage or byte size value,
     * @return the watermark value given
     */
    private static String validWatermarkSetting(String watermark, String settingName) {
        if (definitelyNotPercentage(watermark)) {
            // short circuit to save expensive exception on obvious byte size value below
            ByteSizeValue.parseBytesSizeValue(watermark, settingName);
            return watermark;
        }
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

    // Checks that a value is definitely not a percentage by testing if it ends on `b` which implies that it is probably a byte size value
    // instead. This is used to make setting validation skip attempting to parse a value as a percentage/ration for the settings in this
    // class that accept either a byte size value. The main motivation of this method is to make tests faster. Some tests call this method
    // frequently when starting up internal cluster nodes and using exception throwing and catching when trying to parse as a ratio as a
    // means of identifying that a string is not a ratio is quite slow.
    private static boolean definitelyNotPercentage(String value) {
        return value.endsWith("b");
    }
}

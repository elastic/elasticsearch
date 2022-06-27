/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.DiskThresholdParser;
import org.elasticsearch.core.TimeValue;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

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
        (s) -> DiskThresholdParser.validThresholdSetting(s, "cluster.routing.allocation.disk.watermark.low"),
        new LowDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.high",
        "90%",
        (s) -> DiskThresholdParser.validThresholdSetting(s, "cluster.routing.allocation.disk.watermark.high"),
        new HighDiskWatermarkValidator(),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    public static final Setting<String> CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
        "cluster.routing.allocation.disk.watermark.flood_stage",
        "95%",
        (s) -> DiskThresholdParser.validThresholdSetting(s, "cluster.routing.allocation.disk.watermark.flood_stage"),
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

    private final Collection<ChangedThresholdListener> changedThresholdListeners = new CopyOnWriteArrayList<>();

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
            DiskThresholdParser.doValidate(
                value,
                List.of(
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), value),
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                        highWatermarkRaw
                    ),
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                        floodStageRaw
                    )
                )
            );
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
            DiskThresholdParser.doValidate(
                value,
                List.of(
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                        lowWatermarkRaw
                    ),
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), value),
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(),
                        floodStageRaw
                    )
                )
            );
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
            DiskThresholdParser.doValidate(
                value,
                List.of(
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(),
                        lowWatermarkRaw
                    ),
                    new DiskThresholdParser.ThresholdSetting(
                        CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(),
                        highWatermarkRaw
                    ),
                    new DiskThresholdParser.ThresholdSetting(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), value)
                )
            );
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

    private void setRerouteInterval(TimeValue rerouteInterval) {
        this.rerouteInterval = rerouteInterval;
    }

    private void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    private void setLowWatermark(String lowWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.lowWatermarkRaw = lowWatermark;
        this.freeDiskThresholdLow = 100.0 - DiskThresholdParser.parseThresholdPercentage(lowWatermark);
        this.freeBytesThresholdLow = DiskThresholdParser.parseThresholdBytes(
            lowWatermark,
            CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey()
        );
        changedThresholdListeners.forEach(ChangedThresholdListener::onChange);
    }

    private void setHighWatermark(String highWatermark) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.highWatermarkRaw = highWatermark;
        this.freeDiskThresholdHigh = 100.0 - DiskThresholdParser.parseThresholdPercentage(highWatermark);
        this.freeBytesThresholdHigh = DiskThresholdParser.parseThresholdBytes(
            highWatermark,
            CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey()
        );
        changedThresholdListeners.forEach(ChangedThresholdListener::onChange);
    }

    private void setFloodStage(String floodStageRaw) {
        // Watermark is expressed in terms of used data, but we need "free" data watermark
        this.freeDiskThresholdFloodStage = 100.0 - DiskThresholdParser.parseThresholdPercentage(floodStageRaw);
        this.freeBytesThresholdFloodStage = DiskThresholdParser.parseThresholdBytes(
            floodStageRaw,
            CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey()
        );
        changedThresholdListeners.forEach(ChangedThresholdListener::onChange);
    }

    private void setFrozenFloodStage(RelativeByteSizeValue floodStage) {
        this.frozenFloodStage = floodStage;
        changedThresholdListeners.forEach(ChangedThresholdListener::onChange);
    }

    private void setFrozenFloodStageMaxHeadroom(ByteSizeValue maxHeadroom) {
        this.frozenFloodStageMaxHeadroom = maxHeadroom;
        changedThresholdListeners.forEach(ChangedThresholdListener::onChange);
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

    public RelativeByteSizeValue getFrozenFloodStage() {
        return frozenFloodStage;
    }

    public ByteSizeValue getFrozenFloodStageMaxHeadroom() {
        return frozenFloodStageMaxHeadroom;
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

    public boolean addListener(ChangedThresholdListener changedThresholdListener) {
        return this.changedThresholdListeners.add(changedThresholdListener);
    }

    /**
     * Listening to changes on the raw values of the watermarks.
     */
    public interface ChangedThresholdListener {

        void onChange();
    }
}

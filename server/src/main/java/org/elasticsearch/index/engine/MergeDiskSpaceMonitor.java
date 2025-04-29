/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.Strings;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;

public class MergeDiskSpaceMonitor {
    // TODO make these settings dynamic
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING = new Setting<>(
            "indices.merge.disk.watermark.high",
            "96%",
            (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.high"),
            new ThreadPoolMergeExecutorService.WatermarkValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING = new Setting<>(
            "indices.merge.disk.watermark.high.max_headroom",
            (settings) -> {
                if (INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.exists(settings)) {
                    return "-1";
                } else {
                    return "40GB";
                }
            },
            (s) -> ByteSizeValue.parseBytesSizeValue(s, "indices.merge.disk.watermark.high.max_headroom"),
            new ThreadPoolMergeExecutorService.HeadroomValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
            "indices.merge.disk.watermark.flood_stage",
            "98%",
            (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.flood_stage"),
            new ThreadPoolMergeExecutorService.WatermarkValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING = new Setting<>(
            "indices.merge.disk.watermark.flood_stage.max_headroom",
            (settings) -> {
                if (INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING.exists(settings)) {
                    return "-1";
                } else {
                    return "20GB";
                }
            },
            (s) -> ByteSizeValue.parseBytesSizeValue(s, "indices.merge.disk.watermark.flood_stage.max_headroom"),
            new ThreadPoolMergeExecutorService.HeadroomValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );

    static class WatermarkValidator implements Setting.Validator<RelativeByteSizeValue> {

        @Override
        public void validate(RelativeByteSizeValue value) {
        }

        @Override
        public void validate(final RelativeByteSizeValue value, final Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                throw new IllegalArgumentException("setting a watermark value for indices merge only works when [" +
                        USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey() + "] is set to [true]");
            }
            final RelativeByteSizeValue high = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
            final RelativeByteSizeValue flood = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            // both "watermark" limits must either be absolute or relative, and "high" < "flood"
            if (high.isAbsolute() == false && flood.isAbsolute() == false) { // Validate as percentages
                final double highWatermarkThreshold = high.getRatio().getAsPercent();
                final double floodThreshold = flood.getRatio().getAsPercent();
                if (highWatermarkThreshold > floodThreshold) {
                    throw new IllegalArgumentException(
                            "indices merge high disk watermark ["
                                    + high.getStringRep()
                                    + "] more than flood stage disk watermark ["
                                    + flood.getStringRep()
                                    + "]"
                    );
                }
            } else if (high.isAbsolute() && flood.isAbsolute()) { // Validate as absolute values
                final ByteSizeValue highWatermarkBytes = high.getAbsolute();
                final ByteSizeValue floodStageBytes = flood.getAbsolute();
                if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
                    throw new IllegalArgumentException(
                            "indices merge high disk watermark ["
                                    + high.getStringRep()
                                    + "] less than flood stage disk watermark ["
                                    + flood.getStringRep()
                                    + "]"
                    );
                }
            } else {
                final String message = Strings.format(
                        "unable to consistently parse [%s=%s], and [%s=%s] as percentage or bytes",
                        INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(),
                        high.getStringRep(),
                        INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(),
                        flood.getStringRep()
                );
                throw new IllegalArgumentException(message);
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            List<Setting<?>> res = List.of(
                    INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
                    INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING,
                    USE_THREAD_POOL_MERGE_SCHEDULER_SETTING
            );
            return res.iterator();
        }
    }

    static class HeadroomValidator implements Setting.Validator<ByteSizeValue> {

        @Override
        public void validate(ByteSizeValue value) {
        }

        @Override
        public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent) {
                if (value.equals(ByteSizeValue.MINUS_ONE)) {
                    throw new IllegalArgumentException("setting a headroom value to less than 0 is not supported");
                }
                if (settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                    throw new IllegalArgumentException("setting a headroom value for indices merge only works when [" +
                            USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey() + "] is set to [true]");
                }
            }
            final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING);
            final ByteSizeValue floodHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING);
            final RelativeByteSizeValue high = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
            final RelativeByteSizeValue flood = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            // ensure that if max headroom values are set, then watermark values are ratios/percentages
            if ((high.isAbsolute() || flood.isAbsolute())
                    && (highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false || floodHeadroom.equals(ByteSizeValue.MINUS_ONE) == false)) {
                throw new IllegalArgumentException(
                        "At least one of the indices merge disk max headroom settings is set [high="
                                + highHeadroom.getStringRep()
                                + ", flood="
                                + floodHeadroom.getStringRep()
                                + "], while the indices merge disk watermark values are set to absolute values "
                                + "instead of ratios/percentages [high="
                                + high.getStringRep()
                                + ", flood="
                                + flood.getStringRep()
                                + "]"
                );
            }
            // if the "high" headroom is set, "flood" must also be set
            if (highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false && floodHeadroom.equals(ByteSizeValue.MINUS_ONE)) {
                throw new IllegalArgumentException(
                        "indices merge flood disk max headroom ["
                                + floodHeadroom.getStringRep()
                                + "] is not set, while the indices merge high disk max headroom is set ["
                                + highHeadroom.getStringRep()
                                + "]"
                );
            }
            // the "flood" headroom must be above the "high" one
            if (floodHeadroom.compareTo(highHeadroom) > 0 && highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false) {
                throw new IllegalArgumentException(
                        "indices merge flood disk max headroom ["
                                + floodHeadroom.getStringRep()
                                + "] more than indices merge high disk max headroom ["
                                + highHeadroom.getStringRep()
                                + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            List<Setting<?>> res = List.of(
                    INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
                    INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING,
                    INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
                    INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING,
                    USE_THREAD_POOL_MERGE_SCHEDULER_SETTING
            );
            return res.iterator();
        }
    }

}

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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;

public class MergeDiskSpaceMonitor {

    // TODO make this setting dynamic
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING = new Setting<>(
        "indices.merge.disk.watermark.high",
        "96%",
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.high"),
        new Setting.Validator<>() {
            @Override
            public void validate(RelativeByteSizeValue value) {}

            @Override
            public void validate(RelativeByteSizeValue value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent && settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                    throw new IllegalArgumentException(
                        "indices merge watermark setting is only effective when ["
                            + USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey()
                            + "] is set to [true]"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> res = List.of(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING, USE_THREAD_POOL_MERGE_SCHEDULER_SETTING);
                return res.iterator();
            }
        },
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
        new Setting.Validator<>() {
            @Override
            public void validate(ByteSizeValue value) {}

            @Override
            public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent) {
                    if (value.equals(ByteSizeValue.MINUS_ONE)) {
                        throw new IllegalArgumentException("setting a headroom value to less than 0 is not supported");
                    }
                    if (settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                        throw new IllegalArgumentException(
                            "indices merge max headroom setting is only effective when ["
                                + USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey()
                                + "] is set to [true]"
                        );
                    }
                }
                final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING);
                final RelativeByteSizeValue highWatermark = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
                if (highWatermark.isAbsolute() && highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false) {
                    throw new IllegalArgumentException(
                        "indices merge max headroom setting is set, but disk watermark value is not a relative value"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> res = List.of(
                    INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
                    INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
                    USE_THREAD_POOL_MERGE_SCHEDULER_SETTING
                );
                return res.iterator();
            }
        },
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

}

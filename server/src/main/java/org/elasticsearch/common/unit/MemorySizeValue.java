/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.monitor.jvm.JvmInfo;

import java.util.Objects;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;

/** Utility methods to get memory sizes. */
public enum MemorySizeValue {
    ;

    /** Parse the provided string as a memory size. This method either accepts absolute values such as
     *  {@code 42} (default assumed unit is byte) or {@code 2mb}, or percentages of the heap size: if
     *  the heap is 1G, {@code 10%} will be parsed as {@code 100mb}.  */
    public static ByteSizeValue parseBytesSizeValueOrHeapRatio(String sValue, String settingName) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue != null && sValue.endsWith("%")) {
            return parseHeapRatio(sValue, settingName, 0);
        } else {
            return parseBytesSizeValue(sValue, settingName);
        }
    }

    public static ByteSizeValue parseHeapRatioOrDeprecatedByteSizeValue(String sValue, String settingName, double minHeapPercent) {
        settingName = Objects.requireNonNull(settingName);
        if (sValue != null && sValue.endsWith("%")) {
            return parseHeapRatio(sValue, settingName, minHeapPercent);
        } else {
            DeprecationLogger.getLogger(BalancedShardsAllocator.class)
                .critical(
                    DeprecationCategory.SETTINGS,
                    "absolute_size_not_supported",
                    "[{}] should be specified using a percentage of the heap. Absolute size settings will be forbidden in a future release",
                    settingName
                );
            return parseBytesSizeValue(sValue, settingName);
        }
    }

    private static ByteSizeValue parseHeapRatio(String sValue, String settingName, double minHeapPercent) {
        final String percentAsString = sValue.substring(0, sValue.length() - 1);
        try {
            final double percent = Double.parseDouble(percentAsString);
            if (percent < 0 || percent > 100) {
                throw new ElasticsearchParseException("percentage should be in [0-100], got [{}]", percentAsString);
            } else if (percent < minHeapPercent) {
                DeprecationLogger.getLogger(MemorySizeValue.class)
                    .warn(
                        DeprecationCategory.SETTINGS,
                        "memory_size_below_minimum",
                        "[{}] setting of [{}] is below the recommended minimum of {}% of the heap",
                        settingName,
                        sValue,
                        minHeapPercent
                    );
            }
            return ByteSizeValue.ofBytes((long) ((percent / 100) * JvmInfo.jvmInfo().getMem().getHeapMax().getBytes()));
        } catch (NumberFormatException e) {
            throw new ElasticsearchParseException("failed to parse [{}] as a double", e, percentAsString);
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.core.Nullable;

import java.util.Locale;
import java.util.Map;

/**
 * Configuration for partition detection strategy, parsed from WITH clause parameters.
 * Controls which {@link PartitionDetector} is used and provides an optional path template
 * for template-based detection.
 */
public record PartitionConfig(Strategy strategy, @Nullable String pathTemplate) {

    public enum Strategy {
        AUTO,
        HIVE,
        TEMPLATE,
        NONE;

        /**
         * Case-insensitive parse. Returns {@code null} for null/empty input.
         */
        public static Strategy parse(String value) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return Strategy.valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

    public static final String CONFIG_PARTITIONING_DETECTION = "partition_detection";
    public static final String CONFIG_PARTITIONING_PATH = "partition_path";
    public static final String CONFIG_PARTITIONING_HIVE = "hive_partitioning";

    public static final PartitionConfig DEFAULT = new PartitionConfig(Strategy.AUTO, null);

    public PartitionConfig {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy cannot be null");
        }
    }

    public static PartitionConfig fromConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return DEFAULT;
        }

        Object detectionValue = config.get(CONFIG_PARTITIONING_DETECTION);
        Strategy strategy = detectionValue != null ? Strategy.parse(detectionValue.toString()) : Strategy.AUTO;
        if (strategy == null) {
            strategy = Strategy.AUTO;
        }

        Object templateValue = config.get(CONFIG_PARTITIONING_PATH);
        String template = templateValue != null ? templateValue.toString() : null;

        if (template != null && Strategy.AUTO == strategy) {
            strategy = Strategy.TEMPLATE;
        }

        return new PartitionConfig(strategy, template);
    }
}

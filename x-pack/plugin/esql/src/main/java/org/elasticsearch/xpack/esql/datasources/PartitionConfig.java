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
public record PartitionConfig(String strategy, @Nullable String pathTemplate) {

    public static final String AUTO = "auto";
    public static final String HIVE = "hive";
    public static final String TEMPLATE = "template";
    public static final String NONE = "none";

    public static final String CONFIG_PARTITIONING_DETECTION = "partition_detection";
    public static final String CONFIG_PARTITIONING_PATH = "partition_path";
    public static final String CONFIG_PARTITIONING_HIVE = "hive_partitioning";

    public static final PartitionConfig DEFAULT = new PartitionConfig(AUTO, null);

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
        String strategy = detectionValue != null ? detectionValue.toString().toLowerCase(Locale.ROOT) : AUTO;

        Object templateValue = config.get(CONFIG_PARTITIONING_PATH);
        String template = templateValue != null ? templateValue.toString() : null;

        if (template != null && AUTO.equals(strategy)) {
            strategy = TEMPLATE;
        }

        return new PartitionConfig(strategy, template);
    }
}

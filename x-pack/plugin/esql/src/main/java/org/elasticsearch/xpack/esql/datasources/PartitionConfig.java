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
import java.util.Set;

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

    /** Keys recognised by {@link #fromConfig}. */
    public static final Set<String> CONFIG_KEYS = Set.of(CONFIG_PARTITIONING_DETECTION, CONFIG_PARTITIONING_PATH, CONFIG_PARTITIONING_HIVE);

    public static final PartitionConfig DEFAULT = new PartitionConfig(Strategy.AUTO, null);

    public PartitionConfig {
        if (strategy == null) {
            throw new IllegalArgumentException("strategy cannot be null");
        }
    }

    /**
     * Resolves the three partition settings into one strategy, leniently: this runs on every query against every
     * already-stored dataset, so it never throws on a stored value. Contradictions are rejected at registration
     * instead — see {@link #validate}.
     *
     * <p>Resolution order: the strategy is parsed from {@code partition_detection}; an unparseable value, and an
     * explicit {@code template} with nothing to templatise, fall back to {@code AUTO}; then
     * {@code hive_partitioning: "false"} folds to {@code NONE} last and wins over everything. A
     * {@code partition_path} does NOT promote {@code AUTO} to {@code TEMPLATE} — {@code AUTO} already means Hive
     * first with the template as a fallback, which is what a dataset carrying only a {@code partition_path}
     * resolved to before this setting reached the read path. The final fold is what keeps
     * {@code {hive_partitioning: false, partition_path: ...}} resolving to "no partitions" as it always did.
     *
     * <p>Only the literal {@code "false"} disables detection, matching the boolean check this replaced — any other
     * value (including {@code "yes"}, {@code 0} or a nonsense string) leaves detection enabled, because the setting
     * is stored free-form and existing datasets rely on that reading. An explicit {@code true} carries no
     * information: it is the default, so it is ignored rather than pinning the strategy to {@code HIVE}.
     */
    public static PartitionConfig fromConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return DEFAULT;
        }

        Object detectionValue = config.get(CONFIG_PARTITIONING_DETECTION);
        Strategy strategy = null;
        if (detectionValue != null) {
            try {
                strategy = Strategy.parse(detectionValue.toString());
            } catch (IllegalArgumentException e) {
                // A value stored before the setting was validated as an enum. Reading must not fail on it; the
                // registration path rejects it, and validate() reports it with an actionable message.
                strategy = null;
            }
        }
        if (strategy == null) {
            strategy = Strategy.AUTO;
        }

        Object templateValue = config.get(CONFIG_PARTITIONING_PATH);
        String template = templateValue != null ? templateValue.toString() : null;

        // AUTO is NOT promoted to TEMPLATE when a partition_path is present. AUTO means "Hive first, template as a
        // fallback" (see AutoPartitionDetector), and that is exactly what a dataset carrying only a partition_path
        // resolved to before this setting reached the read path: Hive detection. Promoting here would take the Hive
        // columns away from every such dataset stored against a key=value layout.

        // An explicit TEMPLATE with nothing to templatise falls back to AUTO rather than detecting nothing, so a
        // dataset stored before this combination was rejected at registration keeps the Hive columns it had.
        if (Strategy.TEMPLATE == strategy && (template == null || template.isEmpty())) {
            strategy = Strategy.AUTO;
        }

        if (hivePartitioningDisabled(config)) {
            strategy = Strategy.NONE;
        }

        return new PartitionConfig(strategy, template);
    }

    /** Whether {@code hive_partitioning} is present and set to the literal {@code "false"}. */
    private static boolean hivePartitioningDisabled(Map<String, Object> config) {
        Object value = config.get(CONFIG_PARTITIONING_HIVE);
        return value != null && "false".equalsIgnoreCase(value.toString());
    }

    /**
     * Registration-time validation. Rejects the combinations in which one of the three settings would be silently
     * ignored, so a new dataset cannot be registered with a setting that does nothing. Deliberately stricter than
     * {@link #fromConfig}, which must keep reading datasets that were stored before these checks existed.
     */
    public static void validate(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return;
        }

        Object detectionValue = config.get(CONFIG_PARTITIONING_DETECTION);
        Strategy declared;
        try {
            declared = detectionValue != null ? Strategy.parse(detectionValue.toString()) : null;
        } catch (IllegalArgumentException e) {
            // An unparseable value is already reported by the caller, which validates this key as an enum and
            // produces an actionable message. Rethrowing here would append Enum.valueOf's raw "No enum constant"
            // text as a second error on the same setting.
            return;
        }

        Object templateValue = config.get(CONFIG_PARTITIONING_PATH);
        String template = templateValue != null ? templateValue.toString() : null;
        boolean hasTemplate = template != null && template.isEmpty() == false;

        if (hivePartitioningDisabled(config) && declared != null && declared != Strategy.NONE) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITIONING_HIVE
                    + "] is false, which disables partition detection, but ["
                    + CONFIG_PARTITIONING_DETECTION
                    + "] is ["
                    + declared.name().toLowerCase(Locale.ROOT)
                    + "]; set ["
                    + CONFIG_PARTITIONING_DETECTION
                    + "] to [none] instead of using ["
                    + CONFIG_PARTITIONING_HIVE
                    + "]"
            );
        }

        if (declared == Strategy.TEMPLATE && hasTemplate == false) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITIONING_DETECTION
                    + "] is [template] but no ["
                    + CONFIG_PARTITIONING_PATH
                    + "] was given; template detection needs a path template such as [{year}/{month}]"
            );
        }

        // The hive strategy never reads a path template, so accepting one would store a setting that does nothing —
        // and, before the rewrite was gated on the strategy, one that silently steered the glob. AUTO stays legal:
        // AutoPartitionDetector genuinely consumes the template as its fallback detector.
        if (declared == Strategy.HIVE && hasTemplate) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITIONING_PATH
                    + "] is set but ["
                    + CONFIG_PARTITIONING_DETECTION
                    + "] is [hive], which never reads a path template; set ["
                    + CONFIG_PARTITIONING_DETECTION
                    + "] to [template] or remove ["
                    + CONFIG_PARTITIONING_PATH
                    + "]"
            );
        }

        // A template alongside anything that resolves to "no partitions" is the silent-drop this validation exists
        // to prevent: the template would be accepted, stored, and never used.
        if (hasTemplate && (declared == Strategy.NONE || hivePartitioningDisabled(config))) {
            throw new IllegalArgumentException(
                "["
                    + CONFIG_PARTITIONING_PATH
                    + "] is set but partition detection is disabled, so the template would be ignored; remove ["
                    + CONFIG_PARTITIONING_PATH
                    + "] or enable partition detection"
            );
        }
    }
}

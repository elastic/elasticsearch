/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_DETECTION;
import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_PATH;
import static org.hamcrest.Matchers.containsString;

public class PartitionConfigTests extends ESTestCase {

    public void testDefaultConfig() {
        assertEquals(PartitionConfig.Strategy.AUTO, PartitionConfig.DEFAULT.strategy());
        assertNull(PartitionConfig.DEFAULT.pathTemplate());
    }

    public void testFromEmptyConfig() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of());
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromNullConfig() {
        PartitionConfig config = PartitionConfig.fromConfig(null);
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromConfigWithHiveStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "hive"));
        assertEquals(PartitionConfig.Strategy.HIVE, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromConfigWithTemplateStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(
            Map.of(CONFIG_PARTITIONING_DETECTION, "template", CONFIG_PARTITIONING_PATH, "{year}/{month}")
        );
        assertEquals(PartitionConfig.Strategy.TEMPLATE, config.strategy());
        assertEquals("{year}/{month}", config.pathTemplate());
    }

    public void testFromConfigWithNoneStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "none"));
        assertEquals(PartitionConfig.Strategy.NONE, config.strategy());
    }

    /**
     * A partition_path with no partition_detection stays AUTO. AUTO is "Hive first, template as a fallback", which
     * is what such a dataset resolved to before the setting reached the read path; promoting to TEMPLATE here would
     * take the Hive columns away from every one stored against a key=value layout.
     */
    public void testFromConfigAutoWithTemplateStaysAuto() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_PATH, "{year}/{month}"));
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
        assertEquals("{year}/{month}", config.pathTemplate());
    }

    public void testFromConfigCaseInsensitive() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "HIVE"));
        assertEquals(PartitionConfig.Strategy.HIVE, config.strategy());
    }

    public void testNullStrategyThrows() {
        expectThrows(IllegalArgumentException.class, () -> new PartitionConfig(null, null));
    }

    public void testInvalidStrategyThrows() {
        expectThrows(IllegalArgumentException.class, () -> PartitionConfig.Strategy.parse("banana"));
    }

    public void testEmptyStrategyReturnsNull() {
        assertNull(PartitionConfig.Strategy.parse(""));
    }

    // -- hive_partitioning folding (lenient: runs on every query against every stored dataset) --

    public void testHivePartitioningFalseFoldsToNone() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, "false"));
        assertEquals(PartitionConfig.Strategy.NONE, config.strategy());
    }

    public void testHivePartitioningFalseAsBooleanFoldsToNone() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, false));
        assertEquals(PartitionConfig.Strategy.NONE, config.strategy());
    }

    /**
     * Only the literal "false" disables detection. The setting is stored free-form and the boolean check this
     * replaced treated every other value as enabled, so stored datasets carrying junk must keep reading as they do.
     */
    public void testHivePartitioningNonFalseValuesLeaveDetectionEnabled() {
        for (Object value : List.of("true", true, "yes", "no", 0, 1, "banana")) {
            PartitionConfig config = PartitionConfig.fromConfig(Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, value));
            assertNotEquals("value [" + value + "] must not disable detection", PartitionConfig.Strategy.NONE, config.strategy());
        }
    }

    /** Explicit true carries no information — it is the default — so it must not change the resolved strategy. */
    public void testHivePartitioningTrueDoesNotChangeTheStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(
            Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, true, CONFIG_PARTITIONING_PATH, "{year}")
        );
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
        assertEquals("{year}", config.pathTemplate());
    }

    /** hive_partitioning:false folds last and wins over a path template, so this reads as no-partitions. */
    public void testHivePartitioningFalseBeatsPathTemplate() {
        PartitionConfig config = PartitionConfig.fromConfig(
            Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, "false", CONFIG_PARTITIONING_PATH, "{year}")
        );
        assertEquals(PartitionConfig.Strategy.NONE, config.strategy());
    }

    /** An explicit template with nothing to templatise falls back to AUTO, keeping a stored dataset's columns. */
    public void testTemplateWithoutPathFallsBackToAuto() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "template"));
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
    }

    /** hive never reads a path template, so storing one would store a setting that does nothing. */
    public void testValidateRejectsHiveWithPartitionPath() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "hive", CONFIG_PARTITIONING_PATH, "{year}"))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITIONING_PATH));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITIONING_DETECTION));

        // Reading stays lenient: a dataset stored before this check keeps resolving, it simply never uses the template.
        PartitionConfig stored = PartitionConfig.fromConfig(
            Map.of(CONFIG_PARTITIONING_DETECTION, "hive", CONFIG_PARTITIONING_PATH, "{year}")
        );
        assertEquals(PartitionConfig.Strategy.HIVE, stored.strategy());
        assertEquals("{year}", stored.pathTemplate());
    }

    /** auto genuinely consumes the template as its fallback detector, so it stays legal. */
    public void testValidateAllowsAutoWithPartitionPath() {
        PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "auto", CONFIG_PARTITIONING_PATH, "{year}"));
        PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_PATH, "{year}"));
    }

    /** A value stored before the setting was validated as an enum must not fail the read. */
    public void testUnparseableStoredStrategyFallsBackToAuto() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "banana"));
        assertEquals(PartitionConfig.Strategy.AUTO, config.strategy());
    }

    // -- validate(): registration-time strictness --

    public void testValidateRejectsHiveFalseWithExplicitStrategy() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "hive", PartitionConfig.CONFIG_PARTITIONING_HIVE, "false"))
        );
        assertThat(e.getMessage(), containsString("disables partition detection"));
    }

    public void testValidateAllowsHiveFalseWithExplicitNone() {
        PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "none", PartitionConfig.CONFIG_PARTITIONING_HIVE, "false"));
    }

    public void testValidateRejectsTemplateWithoutPath() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "template"))
        );
        assertThat(e.getMessage(), containsString("needs a path template"));
    }

    public void testValidateRejectsPathAlongsideNone() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "none", CONFIG_PARTITIONING_PATH, "{year}"))
        );
        assertThat(e.getMessage(), containsString("would be ignored"));
    }

    public void testValidateRejectsPathAlongsideHiveFalse() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionConfig.validate(Map.of(PartitionConfig.CONFIG_PARTITIONING_HIVE, "false", CONFIG_PARTITIONING_PATH, "{year}"))
        );
        assertThat(e.getMessage(), containsString("would be ignored"));
    }

    public void testValidateAcceptsHivePartitioningTrueWithAnyStrategy() {
        PartitionConfig.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "hive", PartitionConfig.CONFIG_PARTITIONING_HIVE, true));
        PartitionConfig.validate(
            Map.of(
                CONFIG_PARTITIONING_DETECTION,
                "template",
                CONFIG_PARTITIONING_PATH,
                "{year}",
                PartitionConfig.CONFIG_PARTITIONING_HIVE,
                true
            )
        );
    }

    public void testValidateAcceptsEmptyAndNullSettings() {
        PartitionConfig.validate(Map.of());
        PartitionConfig.validate(null);
    }
}

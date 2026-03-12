/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_DETECTION;
import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_PATH;

public class PartitionConfigTests extends ESTestCase {

    public void testDefaultConfig() {
        assertEquals(PartitionConfig.AUTO, PartitionConfig.DEFAULT.strategy());
        assertNull(PartitionConfig.DEFAULT.pathTemplate());
    }

    public void testFromEmptyConfig() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of());
        assertEquals(PartitionConfig.AUTO, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromNullConfig() {
        PartitionConfig config = PartitionConfig.fromConfig(null);
        assertEquals(PartitionConfig.AUTO, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromConfigWithHiveStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "hive"));
        assertEquals(PartitionConfig.HIVE, config.strategy());
        assertNull(config.pathTemplate());
    }

    public void testFromConfigWithTemplateStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(
            Map.of(CONFIG_PARTITIONING_DETECTION, "template", CONFIG_PARTITIONING_PATH, "{year}/{month}")
        );
        assertEquals(PartitionConfig.TEMPLATE, config.strategy());
        assertEquals("{year}/{month}", config.pathTemplate());
    }

    public void testFromConfigWithNoneStrategy() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "none"));
        assertEquals(PartitionConfig.NONE, config.strategy());
    }

    public void testFromConfigAutoWithTemplatePromotesToTemplate() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_PATH, "{year}/{month}"));
        assertEquals(PartitionConfig.TEMPLATE, config.strategy());
        assertEquals("{year}/{month}", config.pathTemplate());
    }

    public void testFromConfigCaseInsensitive() {
        PartitionConfig config = PartitionConfig.fromConfig(Map.of(CONFIG_PARTITIONING_DETECTION, "HIVE"));
        assertEquals(PartitionConfig.HIVE, config.strategy());
    }

    public void testNullStrategyThrows() {
        expectThrows(IllegalArgumentException.class, () -> new PartitionConfig(null, null));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

/**
 * Unit tests for {@link ParquetRsPlugin}'s feature-flag gating.
 * <p>
 * The prototype parquet-rs reader is gated on the external-datasources umbrella and the
 * {@code esql_external_parquet_rs} sub-flag (snapshot-on, release-off), shared with the
 * {@code reader=parquet-rs} alias in {@link FormatNameResolver}. Across the snapshot and
 * {@code elasticsearch.esql-release} build variants both branches get exercised.
 */
public class ParquetRsPluginTests extends ESTestCase {

    public void testRegistersParquetRsFormatWhenEnabled() {
        assumeTrue("requires parquet-rs feature flag", FormatNameResolver.parquetRsEnabled());
        ParquetRsPlugin plugin = new ParquetRsPlugin();

        assertTrue(
            "should register parquet-rs reader",
            plugin.formatReaders(Settings.EMPTY).containsKey(FormatNameResolver.FORMAT_PARQUET_RS)
        );
        assertTrue(
            "should register parquet-rs format spec",
            plugin.formatSpecs().stream().map(FormatSpec::format).anyMatch(FormatNameResolver.FORMAT_PARQUET_RS::equals)
        );
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when parquet-rs feature flag is off", FormatNameResolver.parquetRsEnabled());
        ParquetRsPlugin plugin = new ParquetRsPlugin();

        assertTrue("no format specs when disabled", plugin.formatSpecs().isEmpty());
        assertTrue("no format readers when disabled", plugin.formatReaders(Settings.EMPTY).isEmpty());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

/**
 * Unit tests for {@link OrcDataSourcePlugin}'s feature-flag gating.
 * <p>
 * ORC registration is gated on the external-datasources umbrella and the {@code esql_external_orc}
 * sub-flag (snapshot-on, release-off). Across the snapshot and {@code elasticsearch.esql-release}
 * build variants both the enabled and disabled branches get exercised.
 */
public class OrcDataSourcePluginTests extends ESTestCase {

    private static boolean orcEnabled() {
        return DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled()
            && OrcDataSourcePlugin.ESQL_EXTERNAL_ORC_FEATURE_FLAG.isEnabled();
    }

    public void testRegistersOrcFormatWhenEnabled() {
        assumeTrue("requires ORC feature flag", orcEnabled());
        OrcDataSourcePlugin plugin = new OrcDataSourcePlugin();

        assertTrue("should register orc reader", plugin.formatReaders(Settings.EMPTY).containsKey("orc"));
        assertTrue("should register orc format spec", plugin.formatSpecs().stream().map(FormatSpec::format).anyMatch("orc"::equals));
    }

    public void testDisabledWhenFeatureFlagOff() {
        assumeFalse("only when ORC feature flag is off", orcEnabled());
        OrcDataSourcePlugin plugin = new OrcDataSourcePlugin();

        assertTrue("no format specs when disabled", plugin.formatSpecs().isEmpty());
        assertTrue("no format readers when disabled", plugin.formatReaders(Settings.EMPTY).isEmpty());
    }
}

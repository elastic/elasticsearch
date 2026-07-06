/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Data source plugin that provides Apache ORC format support for ESQL external data sources.
 *
 * <p>This plugin provides an ORC format reader for reading ORC files from any storage provider.
 * ORC (Optimized Row Columnar) is a columnar storage format optimized for analytics workloads,
 * providing efficient compression and encoding schemes with built-in indexing.
 *
 * <p>Heavy dependencies (ORC, Hadoop) are isolated in this module to avoid jar hell issues
 * in the core ESQL plugin.
 *
 * <p>ORC is not in the released ship set yet, so format registration is gated on the umbrella
 * {@link DatasetMetadata#ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG} and the component
 * {@link #ESQL_EXTERNAL_ORC_FEATURE_FLAG}: the reader is available in snapshot/development builds and
 * disabled in release. When the gate is off the {@code orc} format and {@code .orc} extension are not
 * registered, so an ORC dataset resolves to the generic "No format reader registered" rejection.
 */
public class OrcDataSourcePlugin extends Plugin implements DataSourcePlugin {

    /**
     * Gates the ORC format reader. Snapshot-on, release-off; override in release with
     * {@code -Des.esql_external_orc_feature_flag_enabled=true}.
     */
    public static final FeatureFlag ESQL_EXTERNAL_ORC_FEATURE_FLAG = new FeatureFlag("esql_external_orc");

    private static boolean enabled() {
        return DatasetMetadata.ESQL_EXTERNAL_DATASOURCES_FEATURE_FLAG.isEnabled() && ESQL_EXTERNAL_ORC_FEATURE_FLAG.isEnabled();
    }

    @Override
    public Set<FormatSpec> formatSpecs() {
        if (enabled() == false) {
            return Set.of();
        }
        return Set.of(FormatSpec.of("orc", ".orc"));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        if (enabled() == false) {
            return Map.of();
        }
        return Map.of("orc", (s, blockFactory) -> new OrcFormatReader(blockFactory));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(OrcReaderStatus.ENTRY);
    }
}

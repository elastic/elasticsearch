/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet.parquetrs;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.Map;
import java.util.Set;

/**
 * Data source plugin providing a parquet-rs backed native Parquet reader.
 * <p>
 * Registration is gated on {@link FormatNameResolver#ESQL_EXTERNAL_PARQUET_RS_FEATURE_FLAG} (under the
 * external-datasources umbrella): the reader is a prototype, snapshot-on / release-off. The matching
 * {@code reader=parquet-rs} alias in {@code FormatNameResolver} is gated on the same flag, so exposing
 * {@code format=parquet-rs} would otherwise route queries to a reader that cannot be selected via the
 * public {@code reader} alias in release.
 */
public class ParquetRsPlugin extends Plugin implements DataSourcePlugin {

    @Override
    public Set<FormatSpec> formatSpecs() {
        if (FormatNameResolver.parquetRsEnabled() == false) {
            return Set.of();
        }
        return Set.of(new FormatSpec(FormatNameResolver.FORMAT_PARQUET_RS, Set.of(), Set.of()));
    }

    @Override
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        if (FormatNameResolver.parquetRsEnabled() == false) {
            return Map.of();
        }
        FormatReaderFactory factory = (s, blockFactory) -> new ParquetRsFormatReader(blockFactory);
        return Map.of(FormatNameResolver.FORMAT_PARQUET_RS, factory);
    }
}

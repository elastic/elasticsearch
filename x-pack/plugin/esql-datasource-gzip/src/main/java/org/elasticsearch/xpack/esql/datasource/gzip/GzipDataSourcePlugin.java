/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;

import java.util.List;

/**
 * Data source plugin that provides gzip decompression for ESQL external data sources.
 *
 * <p>Enables compound extensions like {@code .csv.gz}, {@code .tsv.gz}, {@code .ndjson.gz},
 * and {@code .jsonl.gz} by registering a decompression codec for {@code .gz} and {@code .gzip}.
 */
public class GzipDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    public List<DecompressionCodec> decompressionCodecs(Settings settings) {
        return List.of(new GzipDecompressionCodec());
    }
}

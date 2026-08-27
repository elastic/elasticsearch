/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.RemovedParquetDatasetSettings;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/** Pins that the Parquet reader claims no per-dataset configuration keys. */
public class ParquetFormatReaderRecognizedKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testFormatSpecConfigKeysAreEmpty() {
        ParquetDataSourcePlugin plugin = new ParquetDataSourcePlugin();
        Set<FormatSpec> specs = plugin.formatSpecs();
        assertThat(specs.size(), equalTo(1));
        FormatSpec spec = specs.iterator().next();
        assertThat(spec.format(), equalTo(FormatNameResolver.FORMAT_PARQUET));
        assertThat("FormatSpec for [" + spec.format() + "] must declare no config keys", spec.configKeys(), empty());
    }

    public void testWithConfigClaimsNothingIncludingRemovedKeys() {
        ParquetFormatReader reader = new ParquetFormatReader(NOOP_BLOCK_FACTORY);
        Map<String, Object> config = new HashMap<>();
        for (String key : RemovedParquetDatasetSettings.KEYS) {
            config.put(key, randomBoolean());
        }
        config.put("not_a_parquet_key", true);
        Configured<FormatReader> result = reader.withConfigTrackingConsumedKeys(config);
        assertThat(result.consumedKeys(), empty());
        assertThat(result.value(), sameInstance(reader));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(null).consumedKeys(), empty());
    }
}

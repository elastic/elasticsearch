/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.RemovedParquetDatasetSettings;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatSpec;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/** Pins that {@code schema_max_fields} is the only per-dataset configuration key the Parquet reader claims. */
public class ParquetFormatReaderConfigKeysTests extends ESTestCase {

    private static final BlockFactory NOOP_BLOCK_FACTORY = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
        .breaker(new NoopCircuitBreaker("noop"))
        .build();

    public void testFormatSpecConfigKeysAreSchemaMaxFieldsOnly() {
        ParquetDataSourcePlugin plugin = new ParquetDataSourcePlugin();
        Set<FormatSpec> specs = plugin.formatSpecs();
        assertThat(specs.size(), equalTo(1));
        FormatSpec spec = specs.iterator().next();
        assertThat(spec.format(), equalTo(FormatNameResolver.FORMAT_PARQUET));
        assertThat(spec.configKeys(), equalTo(Set.of("schema_max_fields")));
        assertThat(spec.configKeys(), equalTo(ParquetFormatReader.RECOGNIZED_KEYS));
    }

    public void testWithConfigClaimsNothingButSchemaMaxFieldsIncludingRemovedKeys() {
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

    public void testSchemaMaxFieldsIsConsumedAndMovesIdentity() {
        ParquetFormatReader reader = new ParquetFormatReader(NOOP_BLOCK_FACTORY);
        Configured<FormatReader> seven = reader.withConfigTrackingConsumedKeys(Map.of("schema_max_fields", 7));
        assertThat(seven.consumedKeys(), equalTo(Set.of("schema_max_fields")));
        assertThat(seven.identity(), not(equalTo("")));
        assertThat(seven.identity(), not(equalTo(reader.withConfigTrackingConsumedKeys(Map.of("schema_max_fields", 8)).identity())));
    }

    public void testSchemaMaxFieldsIsBounded() {
        for (Object bad : new Object[] { 0, -1, ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS + 1, "many" }) {
            expectThrows(IllegalArgumentException.class, () -> ParquetFormatReader.validateConfig(Map.of("schema_max_fields", bad)));
            expectThrows(
                IllegalArgumentException.class,
                () -> new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of("schema_max_fields", bad))
            );
        }
        ParquetFormatReader.validateConfig(Map.of("schema_max_fields", ExternalSourceSettings.MAX_SCHEMA_MAX_FIELDS));
    }

    public void testEmptyConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(Map.of()).consumedKeys(), empty());
    }

    public void testNullConfigConsumesNothing() {
        assertThat(new ParquetFormatReader(NOOP_BLOCK_FACTORY).withConfigTrackingConsumedKeys(null).consumedKeys(), empty());
    }

    public void testRemovedParquetDatasetSettingsRejectedOnPut() {
        FileDataSourceValidator validator = parquetFormatAwareValidator();
        for (String key : RemovedParquetDatasetSettings.KEYS) {
            var e = expectThrows(
                ValidationException.class,
                () -> validator.validateDataset(Map.of(), "s3://bucket/path/*.parquet", Map.of(key, false))
            );
            assertThat(e.validationErrors(), hasItem(containsString("unknown setting [" + key + "]")));
        }
    }

    private static FileDataSourceValidator parquetFormatAwareValidator() {
        Map<String, Set<String>> formatToConfigKeys = new HashMap<>();
        Map<String, String> extToFormat = new HashMap<>();
        for (FormatSpec spec : new ParquetDataSourcePlugin().formatSpecs()) {
            String format = spec.format().toLowerCase(Locale.ROOT);
            formatToConfigKeys.put(format, spec.configKeys());
            for (String ext : spec.extensions()) {
                String normalized = ext.toLowerCase(Locale.ROOT);
                if (normalized.startsWith(".") == false) {
                    normalized = "." + normalized;
                }
                extToFormat.put(normalized, format);
            }
        }
        return new FileDataSourceValidator("s3", (settings, secrets) -> null, Set.of("s3")).withFormatConfigKeyResolver(
            FileDataSourceValidator.FormatConfigKeyResolver.of(formatToConfigKeys, extToFormat)
        );
    }
}

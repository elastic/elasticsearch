/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.DecompressionCodecRegistry;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileDataSourceValidatorInventoryTests extends ESTestCase {

    public void testFixedAuthModeSurvivesWithers() {
        FileDataSourceValidator validator = new FileDataSourceValidator("http", (settings, secrets) -> null, Set.of("http", "https"))
            .withFixedAuthMode(FileDataSourceConfiguration.AuthMode.ANONYMOUS)
            .withManagedIdentityEnabled(() -> false)
            .withFederatedIdentityEnabled(() -> false)
            .withFormatReaderRegistry(csvGzipRegistry())
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv"))
            );
        assertThat(validator.authModeOrNull(Map.of()), equalTo("anonymous"));
    }

    public void testDatasetShapeCsvGz() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (settings, secrets) -> null, Set.of("s3"))
            .withFormatReaderRegistry(csvGzipRegistry())
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv"))
            );
        DatasetShape shape = validator.datasetShape(Map.of(), "s3://bucket/data.csv.gz");
        assertThat(shape.format(), equalTo("csv"));
        assertThat(shape.compression(), equalTo("gzip"));
    }

    public void testDatasetShapeAutoNeverReported() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (settings, secrets) -> null, Set.of("s3"))
            .withFormatReaderRegistry(csvGzipRegistry())
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv"))
            );
        DatasetShape shape = validator.datasetShape(Map.of("format", "auto"), "s3://bucket/data.csv");
        assertThat(shape.format(), equalTo("csv"));
        assertThat(shape.compression(), equalTo("uncompressed"));
    }

    public void testAuthModeOrNullSwallowsValidationFailure() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (settings, secrets) -> {
            throw new org.elasticsearch.common.ValidationException();
        }, Set.of("s3"));
        assertThat(validator.authModeOrNull(Map.of("region", new DataSourceSetting("us-east-1", false))), nullValue());
    }

    public void testSecretOnlyRebuildPassesExistingSecretKeysLikePutAsUpdate() {
        AtomicBoolean putAsUpdateSplit = new AtomicBoolean();
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (raw, keys) -> {
            putAsUpdateSplit.set(raw != null && raw.isEmpty() && keys.contains("secret_key"));
            return null;
        }, Set.of("s3"));
        String auth = validator.authModeOrNull(Map.of("secret_key", new DataSourceSetting(DataSourceSetting.MASK_SENTINEL, true)));
        assertThat(putAsUpdateSplit.get(), equalTo(true));
        assertThat(auth, equalTo("static_credentials"));
    }

    private static FormatReaderRegistry csvGzipRegistry() {
        FormatReader csv = mock(FormatReader.class);
        when(csv.formatName()).thenReturn("csv");
        when(csv.fileExtensions()).thenReturn(List.of(".csv"));
        when(csv.supportsWholeFileCompression()).thenReturn(true);
        DecompressionCodecRegistry codecs = new DecompressionCodecRegistry();
        codecs.register(new DecompressionCodec() {
            @Override
            public String name() {
                return "gzip";
            }

            @Override
            public List<String> extensions() {
                return List.of(".gz", ".gzip");
            }

            @Override
            public InputStream decompress(InputStream raw) {
                return raw;
            }
        });
        FormatReaderRegistry registry = new FormatReaderRegistry(codecs);
        registry.registerLazy("csv", (s, bf) -> csv, Settings.EMPTY, null);
        registry.registerExtension(".csv", "csv");
        return registry;
    }
}

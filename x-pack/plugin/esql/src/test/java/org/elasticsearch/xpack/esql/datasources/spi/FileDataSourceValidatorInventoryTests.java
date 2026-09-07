/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class FileDataSourceValidatorInventoryTests extends ESTestCase {

    public void testFixedAuthModeSurvivesWithers() {
        FileDataSourceValidator validator = new FileDataSourceValidator("http", (settings, secrets) -> null, Set.of("http", "https"))
            .withFixedAuthMode(FileDataSourceConfiguration.AuthMode.ANONYMOUS)
            .withManagedIdentityEnabled(() -> false)
            .withFederatedIdentityEnabled(() -> false)
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv")),
                Set.of(".gz", ".gzip")
            );
        assertThat(validator.authModeOrNull(Map.of()), equalTo("anonymous"));
    }

    public void testDatasetShapeCsvGz() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (settings, secrets) -> null, Set.of("s3"))
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv")),
                Set.of(".gz", ".gzip")
            );
        DatasetShape shape = validator.datasetShape(Map.of(), "s3://bucket/data.csv.gz");
        assertThat(shape.format(), equalTo("csv"));
        assertThat(shape.compression(), equalTo("gzip"));
    }

    public void testDatasetShapeAutoNeverReported() {
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (settings, secrets) -> null, Set.of("s3"))
            .withFormatConfigKeyResolver(
                FileDataSourceValidator.FormatConfigKeyResolver.of(Map.of("csv", Set.of()), Map.of(".csv", "csv")),
                Set.of(".gz")
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

    public void testSecretOnlyRebuildDoesNotPassEmptyRaw() {
        java.util.concurrent.atomic.AtomicBoolean nonemptyRaw = new java.util.concurrent.atomic.AtomicBoolean();
        FileDataSourceValidator validator = new FileDataSourceValidator("s3", (raw, keys) -> {
            nonemptyRaw.set(raw != null && raw.isEmpty() == false && keys.contains("secret_key"));
            return null;
        }, Set.of("s3"));
        String auth = validator.authModeOrNull(Map.of("secret_key", new DataSourceSetting(DataSourceSetting.MASK_SENTINEL, true)));
        assertThat(nonemptyRaw.get(), equalTo(true));
        assertThat(auth, equalTo("static_credentials"));
    }
}

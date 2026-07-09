/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceSetting;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Per-validator base test. Every {@link DataSourceValidator} implementation should have a sibling
 * test class extending this base.
 *
 * <p>The base enforces the universal validator contract: secret-flag classification flows
 * correctly through both the validator output and the {@code Configuration.toStoredSettings()}
 * path; secret values arrive as {@code String} so the code that encrypts them at PUT time works;
 * the masked display value is wired to the secret flag; null and empty inputs are tolerated;
 * dataset resources are required and scheme-checked; dataset validation errors accumulate rather
 * than fail-fast.
 *
 * <p>Subclasses supply provider-specific inputs through the abstract methods. The base then runs
 * uniform assertions, so every current and future validator inherits the coverage automatically.
 */
public abstract class AbstractDataSourceValidatorTests extends ESTestCase {

    /** Returns the {@link DataSourceValidator} implementation under test. */
    protected abstract DataSourceValidator validator();

    /** Returns the validator's declared type string (e.g. {@code "s3"}, {@code "gcs"}, {@code "azure"}). */
    protected abstract String expectedType();

    /**
     * Returns a minimally-valid input map for the validator that includes every secret-marked field,
     * each with a non-null sample value. Non-secret fields the validator requires for validation must
     * also be present.
     */
    protected abstract Map<String, Object> sampleConfigWithAllSecrets();

    /**
     * The subset of {@link #sampleConfigWithAllSecrets()} keys that the validator's schema declares
     * secret.
     */
    protected abstract Set<String> expectedSecretFieldNames();

    /**
     * Returns a sample valid resource URI for {@link DataSourceValidator#validateDataset}, e.g.
     * {@code "s3://bucket/path/*.parquet"}. Must match a scheme this validator accepts.
     */
    protected abstract String sampleResource();

    /**
     * Returns a resource URI with a scheme this validator does NOT accept (e.g. {@code "gs://..."}
     * for an S3 validator). Used to verify the validator rejects cross-provider schemes.
     */
    protected abstract String wrongSchemeResource();

    /**
     * Routes {@link #sampleConfigWithAllSecrets()} through the provider's {@code Configuration.fromMap}
     * → {@code toStoredSettings()} path and returns the resulting setting map. Companion to
     * {@link #testSecretFlagPropagation()} but exercises the separate code path; the two must classify
     * secrets identically or that's a bug.
     */
    protected abstract Map<String, DataSourceSetting> storedSettingsFromSampleConfig();

    /**
     * Returns dataset settings containing at least {@link #expectedDatasetErrorCount()} fields that
     * fail validation, used to verify the validator accumulates errors rather than failing fast.
     */
    protected abstract Map<String, Object> datasetSettingsWithMultipleErrors();

    /** Number of errors {@link #datasetSettingsWithMultipleErrors()} should produce. Defaults to 2. */
    protected int expectedDatasetErrorCount() {
        return 2;
    }

    public void testSecretFlagPropagation() {
        Map<String, DataSourceSetting> validated = validator().validateDatasource(sampleConfigWithAllSecrets());
        for (String fieldName : expectedSecretFieldNames()) {
            DataSourceSetting setting = validated.get(fieldName);
            assertNotNull("validator dropped field [" + fieldName + "] from its output", setting);
            assertTrue(
                "validator must classify ["
                    + fieldName
                    + "] as secret — found secret=false. "
                    + "If the value is intentionally non-secret, remove it from expectedSecretFieldNames(); "
                    + "otherwise mark it secret in the validator's Configuration schema.",
                setting.secret()
            );
        }
    }

    public void testNonSecretsAreNotMarkedSecret() {
        Map<String, DataSourceSetting> validated = validator().validateDatasource(sampleConfigWithAllSecrets());
        Set<String> expectedSecrets = expectedSecretFieldNames();
        for (var entry : validated.entrySet()) {
            if (expectedSecrets.contains(entry.getKey()) == false) {
                assertFalse(
                    "validator marked ["
                        + entry.getKey()
                        + "] as secret but it isn't in expectedSecretFieldNames(). "
                        + "Either add it to the set or remove the secret-marking from the validator's schema.",
                    entry.getValue().secret()
                );
            }
        }
    }

    /**
     * Ensures the masked display value is wired to the secret flag. If a validator marks a setting
     * secret but the setting's {@code presentationValue()} does not return
     * {@link DataSourceSetting#MASK_SENTINEL}, anywhere that hides the value from the user (GET
     * endpoint, EXPLAIN, logs) would silently leak the plaintext.
     */
    public void testSecretsHavePresentationMaskSentinel() {
        Map<String, DataSourceSetting> validated = validator().validateDatasource(sampleConfigWithAllSecrets());
        for (String key : expectedSecretFieldNames()) {
            assertEquals(
                "validator marked [" + key + "] secret but presentationValue() did not return MASK_SENTINEL",
                DataSourceSetting.MASK_SENTINEL,
                validated.get(key).presentationValue()
            );
        }
    }

    /**
     * The code that encrypts secrets expects every secret value to be a {@code String} so it can be
     * wrapped in {@code EncryptedData}. A validator that produced a non-String secret would crash the
     * encryption call at PUT time.
     */
    public void testSecretValuesAreStrings() {
        Map<String, DataSourceSetting> validated = validator().validateDatasource(sampleConfigWithAllSecrets());
        for (String key : expectedSecretFieldNames()) {
            assertTrue(
                "secret value for [" + key + "] is not a String — the code that encrypts secrets expects a String",
                validated.get(key).rawValue() instanceof String
            );
        }
    }

    /**
     * The validator path ({@code validator.validateDatasource}) and the
     * {@code Configuration.fromMap → toStoredSettings} path must classify secrets identically. If they
     * disagree, a secret stored via one route can leak through the other.
     */
    public void testToStoredSettingsSecretClassification() {
        Map<String, DataSourceSetting> stored = storedSettingsFromSampleConfig();
        Set<String> expectedSecrets = expectedSecretFieldNames();
        for (String key : expectedSecrets) {
            DataSourceSetting setting = stored.get(key);
            assertNotNull("Configuration.toStoredSettings() dropped secret field [" + key + "]", setting);
            assertTrue(
                "Configuration.toStoredSettings() must classify [" + key + "] as secret — divergence from validator path",
                setting.secret()
            );
        }
        for (var entry : stored.entrySet()) {
            if (expectedSecrets.contains(entry.getKey()) == false) {
                assertFalse(
                    "Configuration.toStoredSettings() marked [" + entry.getKey() + "] secret but it isn't a secret field",
                    entry.getValue().secret()
                );
            }
        }
    }

    public void testType() {
        assertEquals(expectedType(), validator().type());
    }

    public void testValidateDatasourceEmpty() {
        assertTrue(validator().validateDatasource(Map.of()).isEmpty());
    }

    public void testValidateDatasourceNullSettings() {
        assertTrue(validator().validateDatasource(null).isEmpty());
    }

    /** Validators must be stateless across calls. Catches hidden caches or accumulators. */
    public void testValidationIdempotent() {
        Map<String, Object> sample = sampleConfigWithAllSecrets();
        Map<String, DataSourceSetting> first = validator().validateDatasource(sample);
        Map<String, DataSourceSetting> second = validator().validateDatasource(sample);
        assertEquals("validator output keysets differ between runs", first.keySet(), second.keySet());
        for (String key : first.keySet()) {
            assertEquals("secret flag differs for [" + key + "] between runs", first.get(key).secret(), second.get(key).secret());
            assertEquals("rawValue differs for [" + key + "] between runs", first.get(key).rawValue(), second.get(key).rawValue());
        }
    }

    /**
     * A request that omits every secret field fails without existing-secret context, but succeeds and does
     * not re-emit the omitted secrets when the caller passes those field names as {@code existingSecretKeys},
     * as {@code DataSourceService} does on a PUT-as-update. Skips validators with no secret fields.
     */
    public void testMergeAwareValidateDatasourceOmitsExistingSecrets() {
        Set<String> secretKeys = expectedSecretFieldNames();
        assumeFalse("validator has no secret fields", secretKeys.isEmpty());
        Map<String, Object> withoutSecrets = new HashMap<>(sampleConfigWithAllSecrets());
        secretKeys.forEach(withoutSecrets::remove);

        expectThrows(ValidationException.class, () -> validator().validateDatasource(withoutSecrets));

        Map<String, DataSourceSetting> merged = validator().validateDatasource(withoutSecrets, secretKeys);
        for (String key : secretKeys) {
            assertFalse(
                "merge-aware validateDatasource should not re-emit an omitted, carried-forward secret [" + key + "]",
                merged.containsKey(key)
            );
        }
    }

    public void testValidateDatasetRequiresResource() {
        expectThrows(ValidationException.class, () -> validator().validateDataset(Map.of(), null, Map.of()));
    }

    public void testValidateDatasetBlankResource() {
        expectThrows(ValidationException.class, () -> validator().validateDataset(Map.of(), "", Map.of()));
    }

    public void testValidateDatasetNullSettings() {
        assertTrue(validator().validateDataset(Map.of(), sampleResource(), null).isEmpty());
    }

    public void testValidateDatasetWrongScheme() {
        expectThrows(ValidationException.class, () -> validator().validateDataset(Map.of(), wrongSchemeResource(), Map.of()));
    }

    public void testValidateDatasetAccumulatesMultipleErrors() {
        var e = expectThrows(
            ValidationException.class,
            () -> validator().validateDataset(Map.of(), sampleResource(), datasetSettingsWithMultipleErrors())
        );
        assertEquals(expectedDatasetErrorCount(), e.validationErrors().size());
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.cluster.metadata.DataSourceSetting;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

/**
 * Per-validator base test. Every {@link DataSourceValidator} implementation should have a sibling
 * test class extending this base.
 *
 * <p>The base provides one universal assertion: a validator that classifies a setting as secret in
 * its declared schema preserves that classification in {@link DataSourceSetting#secret()} on the way
 * out. A validator that drops the {@code secret} flag silently leaks a credential — it bypasses the
 * encryption seam (no encryption happens for non-secret values), the redaction seam (the masking
 * sentinel doesn't kick in), and the warning seam (the plaintext-storage warning checks the same
 * flag). All three rely on the same boolean and must trip together.
 *
 * <p>Subclasses supply three pieces:
 * <ul>
 *   <li>{@link #validator()} — the validator instance under test;
 *   <li>{@link #sampleConfigWithAllSecrets()} — a minimally-valid input map that includes every
 *       field the validator marks secret, each populated with a sample value;
 *   <li>{@link #expectedSecretFieldNames()} — the keys from the sample map that the validator's
 *       schema declares secret. The assertion walks these and confirms each is flagged.
 * </ul>
 */
public abstract class AbstractDataSourceValidatorTests extends ESTestCase {

    /** Returns the {@link DataSourceValidator} implementation under test. */
    protected abstract DataSourceValidator validator();

    /**
     * Returns a minimally-valid input map for the validator that includes every secret-marked field,
     * each with a non-null sample value. Non-secret fields the validator requires for validation must
     * also be present.
     */
    protected abstract Map<String, Object> sampleConfigWithAllSecrets();

    /**
     * The subset of {@link #sampleConfigWithAllSecrets()} keys that the validator's schema declares
     * secret. The base test walks this set and asserts each key comes back with {@code secret() == true}.
     */
    protected abstract Set<String> expectedSecretFieldNames();

    public void testSecretFlagPropagation() {
        Map<String, DataSourceSetting> validated = validator().validateDatasource(sampleConfigWithAllSecrets());
        for (String fieldName : expectedSecretFieldNames()) {
            DataSourceSetting setting = validated.get(fieldName);
            assertNotNull("validator dropped field [" + fieldName + "] from its output", setting);
            assertTrue(
                "validator must classify [" + fieldName + "] as secret — found secret=false. "
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
                    "validator marked [" + entry.getKey() + "] as secret but it isn't in expectedSecretFieldNames(). "
                        + "Either add it to the set or remove the secret-marking from the validator's schema.",
                    entry.getValue().secret()
                );
            }
        }
    }
}

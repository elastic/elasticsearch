/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class FIPS140PasswordHashingAlgorithmBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testPBKDF2AlgorithmIsAllowed() {
        {
            final Settings settings = Settings.builder()
                    .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
                    .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2_10000")
                    .build();
            final BootstrapCheck.BootstrapCheckResult result =
                    new FIPS140PasswordHashingAlgorithmBootstrapCheck().check(createTestContext(settings, null));
            assertFalse(result.isFailure());
        }

        {
            final Settings settings = Settings.builder()
                    .put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true)
                    .put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2")
                    .build();
            final BootstrapCheck.BootstrapCheckResult result =
                    new FIPS140PasswordHashingAlgorithmBootstrapCheck().check(createTestContext(settings, null));
            assertFalse(result.isFailure());
        }
    }

    public void testBCRYPTAlgorithmDependsOnFipsMode() {
        for (final Boolean fipsModeEnabled : Arrays.asList(true, false)) {
            for (final String passwordHashingAlgorithm : Arrays.asList(null, "BCRYPT", "BCRYPT11")) {
                runBCRYPTTest(fipsModeEnabled, passwordHashingAlgorithm);
            }
        }
    }

    private void runBCRYPTTest(final boolean fipsModeEnabled, final String passwordHashingAlgorithm) {
        final Settings.Builder builder = Settings.builder().put(XPackSettings.FIPS_MODE_ENABLED.getKey(), fipsModeEnabled);
        if (passwordHashingAlgorithm != null) {
            builder.put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), passwordHashingAlgorithm);
        }
        final Settings settings = builder.build();
        final BootstrapCheck.BootstrapCheckResult result =
                new FIPS140PasswordHashingAlgorithmBootstrapCheck().check(createTestContext(settings, null));
        assertThat(result.isFailure(), equalTo(fipsModeEnabled));
    }

}

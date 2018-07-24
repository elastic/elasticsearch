/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;

public class FIPS140PasswordHashingAlgorithmBootstrapCheckTests extends ESTestCase {

    public void testPBKDF2AlgorithmIsAllowed() {
        Settings settings = Settings.builder().put("xpack.security.fips_mode.enabled", "true").build();

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2_10000").build();
        assertFalse(new FIPS140PasswordHashingAlgorithmBootstrapCheck(settings).check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2").build();
        assertFalse(new FIPS140PasswordHashingAlgorithmBootstrapCheck(settings).check(new BootstrapContext(settings, null)).isFailure());
    }

    public void testBCRYPTAlgorithmIsNotAllowed() {
        Settings settings = Settings.builder().put("xpack.security.fips_mode.enabled", "true").build();
        assertTrue(new FIPS140PasswordHashingAlgorithmBootstrapCheck(settings).check(new BootstrapContext(settings, null)).isFailure());
        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT").build();
        assertTrue(new FIPS140PasswordHashingAlgorithmBootstrapCheck(settings).check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT11").build();
        assertTrue(new FIPS140PasswordHashingAlgorithmBootstrapCheck(settings).check(new BootstrapContext(settings, null)).isFailure());
    }
}

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

import javax.crypto.SecretKeyFactory;
import java.security.NoSuchAlgorithmException;

public class PasswordHashingAlgorithmBootstrapCheckTests extends ESTestCase {

    public void testPasswordHashingAlgorithmBootstrapCheck() {
        Settings settings = Settings.EMPTY;
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
        // The following two will always pass because for now we only test in environments where PBKDF2WithHMACSHA512 is supported
        assertTrue(isSecretkeyFactoryAlgoAvailable("PBKDF2WithHMACSHA512"));
        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2_10000").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "PBKDF2").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());

        settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), "BCRYPT11").build();
        assertFalse(new PasswordHashingAlgorithmBootstrapCheck().check(new BootstrapContext(settings, null)).isFailure());
    }

    private boolean isSecretkeyFactoryAlgoAvailable(String algorithmId) {
        try {
            SecretKeyFactory.getInstance(algorithmId);
            return true;
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }
}

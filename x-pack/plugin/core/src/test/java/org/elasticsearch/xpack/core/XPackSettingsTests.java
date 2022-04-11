/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.Build;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.security.NoSuchAlgorithmException;

import javax.crypto.SecretKeyFactory;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.DOMAIN_TO_REALM_ASSOC_SETTING;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

public class XPackSettingsTests extends ESTestCase {

    public void testDefaultSSLCiphers() {
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_128_CBC_SHA"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_256_CBC_SHA"));
    }

    public void testChaCha20InCiphersOnJdk12Plus() {
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_CHACHA20_POLY1305_SHA256"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"));
    }

    public void testPasswordHashingAlgorithmSettingValidation() {
        final boolean isPBKDF2Available = isSecretkeyFactoryAlgoAvailable("PBKDF2WithHMACSHA512");
        final String pbkdf2Algo = randomFrom("PBKDF2_10000", "PBKDF2");
        final Settings settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), pbkdf2Algo).build();
        if (isPBKDF2Available) {
            assertEquals(pbkdf2Algo, XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
        } else {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings)
            );
            assertThat(e.getMessage(), containsString("Support for PBKDF2WithHMACSHA512 must be available"));
        }

        final String bcryptAlgo = randomFrom("BCRYPT", "BCRYPT11");
        assertEquals(
            bcryptAlgo,
            XPackSettings.PASSWORD_HASHING_ALGORITHM.get(
                Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), bcryptAlgo).build()
            )
        );
    }

    public void testDefaultPasswordHashingAlgorithmInFips() {
        final Settings.Builder builder = Settings.builder();
        if (inFipsJvm()) {
            builder.put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true);
            assertThat(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(builder.build()), equalTo("PBKDF2_STRETCH"));
        } else {
            assertThat(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(builder.build()), equalTo("BCRYPT"));
        }
    }

    public void testDefaultSupportedProtocols() {
        if (inFipsJvm()) {
            assertThat(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS, contains("TLSv1.2", "TLSv1.1"));
        } else {
            assertThat(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS, contains("TLSv1.3", "TLSv1.2", "TLSv1.1"));

        }
    }

    public void testServiceTokenHashingAlgorithmSettingValidation() {
        final boolean isPBKDF2Available = isSecretkeyFactoryAlgoAvailable("PBKDF2WithHMACSHA512");
        final String pbkdf2Algo = randomFrom("PBKDF2_10000", "PBKDF2", "PBKDF2_STRETCH");
        final Settings settings = Settings.builder().put(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(), pbkdf2Algo).build();
        if (isPBKDF2Available) {
            assertEquals(pbkdf2Algo, XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(settings));
        } else {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(settings)
            );
            assertThat(e.getMessage(), containsString("Support for PBKDF2WithHMACSHA512 must be available"));
        }

        final String bcryptAlgo = randomFrom("BCRYPT", "BCRYPT11");
        assertEquals(
            bcryptAlgo,
            XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(
                Settings.builder().put(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(), bcryptAlgo).build()
            )
        );
    }

    public void testDefaultServiceTokenHashingAlgorithm() {
        assertThat(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(Settings.EMPTY), equalTo("PBKDF2_STRETCH"));
    }

    // Whether the domain setting is registered by default depends on the build type
    public void testRealmDomainSettingRegistrationDefault() {
        assertThat(
            XPackSettings.getAllSettings()
                .stream()
                .filter(setting -> setting.getKey().equals(DOMAIN_TO_REALM_ASSOC_SETTING.getKey()))
                .toList(),
            hasSize(Build.CURRENT.isSnapshot() ? 1 : 0)
        );
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

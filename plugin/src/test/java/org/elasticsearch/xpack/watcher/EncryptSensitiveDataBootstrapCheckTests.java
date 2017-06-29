/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.crypto.CryptoService;

public class EncryptSensitiveDataBootstrapCheckTests extends ESTestCase {

    public void testDefaultIsFalse() {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        Environment env = new Environment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(settings, env);
        assertFalse(check.check());
        assertTrue(check.alwaysEnforce());
    }

    public void testNoKeyInKeystore() {
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true)
                .build();
        Environment env = new Environment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(settings, env);
        assertTrue(check.check());
    }

    public void testKeyInKeystore() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(Watcher.ENCRYPTION_KEY_SETTING.getKey(), CryptoService.generateKey());
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true)
                .setSecureSettings(secureSettings)
                .build();
        Environment env = new Environment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(settings, env);
        assertFalse(check.check());
    }
}

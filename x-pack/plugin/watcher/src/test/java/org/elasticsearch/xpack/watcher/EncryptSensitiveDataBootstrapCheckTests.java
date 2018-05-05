/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoServiceTests;

public class EncryptSensitiveDataBootstrapCheckTests extends ESTestCase {

    public void testDefaultIsFalse() {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(env);
        assertFalse(check.check(new BootstrapContext(settings, null)).isFailure());
        assertTrue(check.alwaysEnforce());
    }

    public void testNoKeyInKeystore() {
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true)
                .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(env);
        assertTrue(check.check(new BootstrapContext(settings, null)).isFailure());
    }

    public void testKeyInKeystore() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), CryptoServiceTests.generateKey());
        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true)
                .setSecureSettings(secureSettings)
                .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        EncryptSensitiveDataBootstrapCheck check = new EncryptSensitiveDataBootstrapCheck(env);
        assertFalse(check.check(new BootstrapContext(settings, null)).isFailure());
    }

}

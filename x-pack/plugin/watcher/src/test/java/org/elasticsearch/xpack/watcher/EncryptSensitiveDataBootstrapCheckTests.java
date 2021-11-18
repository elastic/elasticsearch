/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.elasticsearch.xpack.core.watcher.crypto.CryptoServiceTests;

public class EncryptSensitiveDataBootstrapCheckTests extends AbstractBootstrapCheckTestCase {
    private static final EncryptSensitiveDataBootstrapCheck CHECK = new EncryptSensitiveDataBootstrapCheck();

    public void testDefaultIsFalse() {
        assertFalse(CHECK.check(emptyContext).isFailure());
        assertTrue(CHECK.alwaysEnforce());
    }

    public void testNoKeyInKeystore() {
        Settings settings = Settings.builder().put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true).build();
        assertTrue(CHECK.check(createTestContext(settings, null)).isFailure());
    }

    public void testKeyInKeystore() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), CryptoServiceTests.generateKey());
        Settings settings = Settings.builder()
            .put(Watcher.ENCRYPT_SENSITIVE_DATA_SETTING.getKey(), true)
            .setSecureSettings(secureSettings)
            .build();
        assertFalse(CHECK.check(createTestContext(settings, null)).isFailure());
    }

}

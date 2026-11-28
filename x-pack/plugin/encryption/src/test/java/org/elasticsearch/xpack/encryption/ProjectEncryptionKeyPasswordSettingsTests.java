/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ProjectEncryptionKeyPasswordSettingsTests extends ESTestCase {

    public void testActivePasswordIdAbsent() {
        assertThat(ProjectEncryptionKeyPasswordSettings.getActivePasswordId(Settings.EMPTY), nullValue());
    }

    public void testActivePasswordIdReturnedWhenSet() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v1");
        Settings settings = Settings.builder().setSecureSettings(secure).build();

        assertThat(ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settings), equalTo("v1"));
    }

    public void testPasswordAbsent() {
        assertThat(ProjectEncryptionKeyPasswordSettings.getPassword(Settings.EMPTY, "v1"), nullValue());
    }

    public void testPasswordReturnedWhenSetForId() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1", "p4ssw0rd");
        Settings settings = Settings.builder().setSecureSettings(secure).build();

        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(settings, "v1")) {
            assertThat(password, equalTo(new SecureString("p4ssw0rd".toCharArray())));
        }
    }

    public void testPasswordIsScopedById() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1", "p1");
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v2", "p2");
        Settings settings = Settings.builder().setSecureSettings(secure).build();

        try (
            SecureString p1 = ProjectEncryptionKeyPasswordSettings.getPassword(settings, "v1");
            SecureString p2 = ProjectEncryptionKeyPasswordSettings.getPassword(settings, "v2")
        ) {
            assertThat(p1, equalTo(new SecureString("p1".toCharArray())));
            assertThat(p2, equalTo(new SecureString("p2".toCharArray())));
        }

        assertThat(ProjectEncryptionKeyPasswordSettings.getPassword(settings, "v3"), nullValue());
    }

    public void testCanWrapForDiskRequiresActiveIdAndItsPassword() {
        assertFalse(ProjectEncryptionKeyPasswordSettings.canWrapForDisk(Settings.EMPTY));
        assertFalse(ProjectEncryptionKeyPasswordSettings.canWrapForDisk(settings("v1", "v2")));
        assertFalse(ProjectEncryptionKeyPasswordSettings.canWrapForDisk(settings(null, "v1")));
        assertTrue(ProjectEncryptionKeyPasswordSettings.canWrapForDisk(settings("v1", "v1")));
    }

    public void testValidateReloadRejectsLosingNeededPassword() {
        assertRejected(settings(null, "v1"), ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY);
        assertRejected(settings("v1"), ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1");
        assertRejected(settings("v2", "v1"), ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v2");
        assertRejected(settings("v2", "v2"), ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1");
    }

    private static void assertRejected(Settings updated, String missingSetting) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ProjectEncryptionKeyPasswordSettings.validateReload(settings("v1", "v1"), updated, "v1")
        );
        assertThat(e.getMessage(), containsString("[" + missingSetting + "]"));
    }

    /** Rotating while keeping the gateway copy's password, nothing persisted, or a node that could not wrap before. */
    public void testValidateReloadAllowsChangesThatKeepNeededPasswords() {
        ProjectEncryptionKeyPasswordSettings.validateReload(settings("v1", "v1"), settings("v2", "v1", "v2"), "v1");
        ProjectEncryptionKeyPasswordSettings.validateReload(settings("v2", "v1", "v2"), settings("v2", "v2"), "v2");
        ProjectEncryptionKeyPasswordSettings.validateReload(settings("v1", "v1"), Settings.EMPTY, null);
        ProjectEncryptionKeyPasswordSettings.validateReload(settings("v1"), settings("v1", "v1"), "v1");
        ProjectEncryptionKeyPasswordSettings.validateReload(settings("v2", "v2"), settings("v2", "v2"), "v1");
    }

    private static Settings settings(String activePasswordId, String... passwordIds) {
        MockSecureSettings secure = new MockSecureSettings();
        if (activePasswordId != null) {
            secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, activePasswordId);
        }
        for (String id : passwordIds) {
            secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + id, "password-for-" + id);
        }
        return Settings.builder().setSecureSettings(secure).build();
    }
}

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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ProjectEncryptionKeyPasswordSettingsTests extends ESTestCase {

    private static final String ACTIVE_ID_KEY = ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY;
    private static final String PREFIX = ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX;

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

    public void testWrapForGatewayFailure() {
        assertThat(ProjectEncryptionKeyPasswordSettings.wrapForGatewayFailure(Settings.EMPTY), containsString(ACTIVE_ID_KEY));
        assertThat(ProjectEncryptionKeyPasswordSettings.wrapForGatewayFailure(settings("v1", "v2")), containsString(PREFIX + "v1"));
        assertThat(ProjectEncryptionKeyPasswordSettings.wrapForGatewayFailure(settings("v1", "v1")), nullValue());
    }

    /** FIPS needs at least 112 bits of password, so a configured password can still be unable to wrap the key. */
    public void testWrapForGatewayFailsForPasswordTooShortForFips() {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ACTIVE_ID_KEY, "v1");
        secure.setString(PREFIX + "v1", "short");
        String failure = ProjectEncryptionKeyPasswordSettings.wrapForGatewayFailure(Settings.builder().setSecureSettings(secure).build());
        if (inFipsJvm()) {
            assertThat(failure, containsString("cannot be used to wrap the key"));
        } else {
            assertThat(failure, nullValue());
        }
    }

    public void testValidateReloadRejectsLosingTheActivePassword() {
        assertRejected(settings(null, "v1"), ACTIVE_ID_KEY);
        assertRejected(settings("v2", "v1"), PREFIX + "v2");
    }

    /** Rotating to a configured password, or a node that could not wrap before. */
    public void testValidateReloadAllowsChangesThatKeepTheActivePassword() {
        ProjectEncryptionKeyPasswordSettings.validateReload(true, settings("v2", "v1", "v2"));
        ProjectEncryptionKeyPasswordSettings.validateReload(false, Settings.EMPTY);
    }

    private static void assertRejected(Settings updated, String missingSetting) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ProjectEncryptionKeyPasswordSettings.validateReload(true, updated)
        );
        assertThat(e.getMessage(), containsString("[" + missingSetting + "]"));
        assertThat(e.getMessage(), containsString("keeping the previous settings"));
    }

    private static Settings settings(String activePasswordId, Map<String, String> passwords) {
        MockSecureSettings secure = new MockSecureSettings();
        if (activePasswordId != null) {
            secure.setString(ACTIVE_ID_KEY, activePasswordId);
        }
        passwords.forEach((id, password) -> secure.setString(PREFIX + id, password));
        return Settings.builder().setSecureSettings(secure).build();
    }

    private static Settings settings(String activePasswordId, String... passwordIds) {
        return settings(activePasswordId, Arrays.stream(passwordIds).collect(Collectors.toMap(id -> id, id -> "password-for-" + id)));
    }
}

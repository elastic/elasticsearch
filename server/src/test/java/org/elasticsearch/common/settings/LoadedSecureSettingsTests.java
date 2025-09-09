/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class LoadedSecureSettingsTests extends ESTestCase {

    public void testCopiesMatchingSecureSettings() throws GeneralSecurityException, IOException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");
        mockSecureSettings.setString("secure.api_key", "abcd1234");

        var settings = Settings.builder().put("some.other", "value").setSecureSettings(mockSecureSettings).build();

        var securePasswordSetting = SecureSetting.secureString("secure.password", null);
        var secureApiKeySetting = SecureSetting.secureString("secure.api_key", null);

        var loaded = LoadedSecureSettings.toLoadedSecureSettings(settings, List.of(securePasswordSetting, secureApiKeySetting));
        mockSecureSettings.close();

        assertTrue(loaded.isLoaded());
        assertThat(loaded.getSettingNames(), containsInAnyOrder("secure.password", "secure.api_key"));
        assertThat(loaded.getString("secure.password").toString(), equalTo("changeme"));
        assertThat(loaded.getString("secure.api_key").toString(), equalTo("abcd1234"));

        assertThat(loaded.getSHA256Digest("secure.password"), notNullValue());
        assertThat(loaded.getSHA256Digest("secure.api_key"), notNullValue());
    }

    public void testIgnoresNonMatchingSettings() throws GeneralSecurityException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        var differentSetting = SecureSetting.secureString("secure.token", null);

        var loaded = LoadedSecureSettings.toLoadedSecureSettings(settings, List.of(differentSetting));
        assertThat(loaded.getSettingNames().isEmpty(), equalTo(true));
    }

    public void testFileSettingThrows() throws GeneralSecurityException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("secure.file", randomByteArrayOfLength(16));
        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();

        var loaded = LoadedSecureSettings.toLoadedSecureSettings(settings, List.of());

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> loaded.getFile("secure.file"));
        assertThat(ex.getMessage(), equalTo("A loaded SecureSetting cannot be a file"));
    }

    public void testWriteToThrows() throws Exception {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.secret", "topsecret");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        var loaded = LoadedSecureSettings.toLoadedSecureSettings(settings, List.of());

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> loaded.writeTo(null));
        assertThat(ex.getMessage(), equalTo("A loaded SecureSetting cannot be serialized"));
    }

    public void testNullSourceOrSettingsList() throws Exception {
        var empty = Settings.EMPTY;

        var loaded = LoadedSecureSettings.toLoadedSecureSettings(empty, null);
        assertThat(loaded.isLoaded(), equalTo(true));
        assertThat(loaded.getSettingNames().isEmpty(), equalTo(true));

        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();

        var loaded2 = LoadedSecureSettings.toLoadedSecureSettings(settings, null);
        assertTrue(loaded2.getSettingNames().isEmpty());
    }
}

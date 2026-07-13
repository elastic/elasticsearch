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

public class InMemoryClonedSecureSettingsTests extends ESTestCase {

    public void testClonesMatchingSecureSettings() throws GeneralSecurityException, IOException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");
        mockSecureSettings.setString("secure.api_key", "abcd1234");

        var settings = Settings.builder().put("some.other", "value").setSecureSettings(mockSecureSettings).build();

        var securePasswordSetting = SecureSetting.secureString("secure.password", null);
        var secureApiKeySetting = SecureSetting.secureString("secure.api_key", null);

        var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, List.of(securePasswordSetting, secureApiKeySetting));
        mockSecureSettings.close();

        assertTrue(cloned.isLoaded());
        assertThat(cloned.getSettingNames(), containsInAnyOrder("secure.password", "secure.api_key"));
        assertThat(cloned.getString("secure.password").toString(), equalTo("changeme"));
        assertThat(cloned.getString("secure.api_key").toString(), equalTo("abcd1234"));

        assertThat(cloned.getSHA256Digest("secure.password"), notNullValue());
        assertThat(cloned.getSHA256Digest("secure.api_key"), notNullValue());
    }

    public void testIgnoresNonMatchingSettings() throws GeneralSecurityException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        var differentSetting = SecureSetting.secureString("secure.token", null);

        var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, List.of(differentSetting));
        assertThat(cloned.getSettingNames().isEmpty(), equalTo(true));
    }

    public void testFileSettingThrows() throws GeneralSecurityException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("secure.file", randomByteArrayOfLength(16));
        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();

        var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, List.of());

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> cloned.getFile("secure.file"));
        assertThat(ex.getMessage(), equalTo("A cloned SecureSetting cannot be a file"));
    }

    public void testWriteToThrows() throws Exception {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.secret", "topsecret");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, List.of());

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> cloned.writeTo(null));
        assertThat(ex.getMessage(), equalTo("A cloned SecureSetting cannot be serialized"));
    }

    public void testNullSourceOrSettingsList() throws Exception {
        var empty = Settings.EMPTY;

        {
            var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(empty, null);
            assertThat(cloned.isLoaded(), equalTo(true));
            assertThat(cloned.getSettingNames().isEmpty(), equalTo(true));

        }
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");

        var settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        {
            var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, null);
            assertTrue(cloned.getSettingNames().isEmpty());
        }
    }

    public void testClonesDoNotCloseSource() throws GeneralSecurityException, IOException {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setString("secure.password", "changeme");
        var settings = Settings.builder().put("some.other", "value").setSecureSettings(mockSecureSettings).build();
        var securePasswordSetting = SecureSetting.secureString("secure.password", null);
        var cloned = InMemoryClonedSecureSettings.cloneSecureSettings(settings, List.of(securePasswordSetting));
        mockSecureSettings.close();

        {
            SecureString clonedSecureString = cloned.getString("secure.password");
            assertArrayEquals(clonedSecureString.getChars(), "changeme".toCharArray());
            clonedSecureString.close();
            var exception = assertThrows(IllegalStateException.class, clonedSecureString::getChars);
            assertThat(exception.getMessage(), equalTo("SecureString has already been closed"));
        }
        {
            SecureString clonedSecureString = cloned.getString("secure.password");
            assertArrayEquals(clonedSecureString.getChars(), "changeme".toCharArray());
            assertFalse(clonedSecureString.isEmpty());
        }
    }

}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SecureStringTests extends ESTestCase {

    public void testCloseableCharsDoesNotModifySecureString() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        try (SecureString copy = secureString.clone()) {
            assertArrayEquals(password, copy.getChars());
            assertThat(copy.getChars(), not(sameInstance(password)));
        }
        assertSecureStringEqualToChars(password, secureString);
    }

    public void testClosingSecureStringDoesNotModifyCloseableChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        final char[] passwordCopy = Arrays.copyOf(password, password.length);
        assertArrayEquals(password, passwordCopy);
        secureString.close();
        assertNotEquals(password[0], passwordCopy[0]);
        assertArrayEquals(passwordCopy, copy.getChars());
    }

    public void testClosingChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        copy.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            copy.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, copy::getChars);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    public void testGetCloseableCharsAfterSecureStringClosed() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        secureString.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            secureString.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, secureString::clone);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    private void assertSecureStringEqualToChars(char[] expected, SecureString secureString) {
        int pos = 0;
        for (int i : secureString.chars().toArray()) {
            if (pos >= expected.length) {
                fail("Index " + i + " greated than or equal to array length " + expected.length);
            } else {
                assertEquals(expected[pos++], (char) i);
            }
        }
    }

    /**
     * Verify deprecated insecureSetting can be in Settings or SecureSettings.
     * Verify non-deprecated insecureSetting can only be in Settings, not in SecureSettings.
     */
    public void testInsecureStringSetting() {
        final String deprecatedName = "deprecated";
        final String notDeprecatedName = "notDeprecated";
        final String deprecatedValue = randomAlphaOfLength(4);
        final String notDeprecatedValue = randomAlphaOfLength(4);

        @SuppressWarnings("deprecation")
        final Setting<SecureString> deprecatedSetting = SecureSetting.insecureString(notDeprecatedName);
        final Setting<SecureString> notDeprecatedSetting = SecureSetting.insecureStringNotDeprecated(notDeprecatedName);

        // get defaults for both from settings
        final SecureString deprecatedDefaultValue = deprecatedSetting.getDefault(Settings.EMPTY);
        final SecureString notDeprecatedDefaultValue = notDeprecatedSetting.getDefault(Settings.EMPTY);
        assertThat(deprecatedDefaultValue, is(equalTo("")));
        assertThat(notDeprecatedDefaultValue, is(equalTo("")));

        // get values for both from settings
        final SecureString notDeprecatedSettingValue = notDeprecatedSetting.get(
            Settings.builder().put(notDeprecatedName, notDeprecatedValue).build()
        );
        final SecureString deprecatedSettingValue = deprecatedSetting.get(Settings.builder().put(deprecatedName, deprecatedValue).build());
        assertThat(notDeprecatedSettingValue, is(equalTo(notDeprecatedValue)));
        assertThat(deprecatedSettingValue, is(equalTo(deprecatedSettingValue)));

        // get value for deprecated setting from secure settings, it works because it is deprecated
        final SecureString deprecatedSecureSettingValue = deprecatedSetting.get(
            Settings.builder()
                .setSecureSettings(new SecureSettingsBackedByMap(Map.of(deprecatedName, deprecatedValue.toCharArray())))
                .build()
        );
        assertThat(deprecatedSecureSettingValue, is(equalTo(deprecatedSettingValue)));
        // get value for not deprecated setting from secure settings, it fails because it is not deprecated
        final IllegalArgumentException notDeprecatedException = expectThrows(
            IllegalArgumentException.class,
            () -> notDeprecatedSetting.get(
                Settings.builder()
                    .setSecureSettings(new SecureSettingsBackedByMap(Map.of(notDeprecatedName, notDeprecatedValue.toCharArray())))
                    .build()
            )
        );
        assertThat(
            notDeprecatedException.getMessage(),
            is(
                equalTo(
                    "Setting ["
                        + notDeprecatedName
                        + "] is a non-secure setting and must be stored inside elasticsearch.yml, "
                        + "but was found inside the Elasticsearch keystore"
                )
            )
        );
    }

    private static class SecureSettingsBackedByMap implements SecureSettings {
        private final Map<String, char[]> map;

        SecureSettingsBackedByMap(final Map<String, char[]> map) {
            this.map = Map.copyOf(map);
        }

        @Override
        public boolean isLoaded() {
            return this.map != null;
        }

        @Override
        public SecureString getString(final String setting) {
            return ((this.map == null) || (this.map.get(setting) == null)) ? null : new SecureString(this.map.get(setting));
        }

        @Override
        public Set<String> getSettingNames() {
            return (this.map == null) ? null : this.map.keySet();
        }

        @Override
        public InputStream getFile(String setting) {
            return null;
        }

        @Override
        public byte[] getSHA256Digest(final String setting) {
            return MessageDigests.sha256().digest(new String(this.map.get(setting)).getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            if (this.map != null) {
                for (final char[] value : this.map.values()) {
                    if (value != null) {
                        Arrays.fill(value, " ".charAt(0));
                    }
                }
            }
        }
    }
}

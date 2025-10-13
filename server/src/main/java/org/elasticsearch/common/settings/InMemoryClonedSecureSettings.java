/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class InMemoryClonedSecureSettings {

    /**
     * Creates a cloned (detached) {@link SecureSettings} instance by copying selected secure settings from the provided {@link Settings}.
     * The returned instance does not require the original {@link SecureSettings} to remain open and will always report as loaded.
     * <p>
     * Only secure settings of type {@code String} are supported (file-based secure settings are not). The returned instance cannot be
     * serialized. Also, attempting to {@code close} it will not have any effect.
     * <p>
     * The cloned secure settings will remain in memory for the lifetime of the returned object. This bypasses the normal lifecycle of
     * {@link SecureSettings}. Great care must be taken when using this method to avoid unintentionally retaining sensitive data in memory.
     *
     * @param source          the {@link Settings} object with open/available {@link SecureSettings}
     * @param settingsToClone the list of secure settings definitions to copy if present
     * @return a cloned {@link SecureSettings} containing only the selected settings if present
     * @throws GeneralSecurityException if any secure setting cannot be accessed
     */

    public static SecureSettings cloneSecureSettings(Settings source, List<Setting<?>> settingsToClone) throws GeneralSecurityException {
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        final Map<String, SecureSettingValue> clonedSettings = new HashMap<>();

        if (sourceSecureSettings != null && settingsToClone != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : settingsToClone) {
                    if (secureSetting.match(settingKey)) {
                        clonedSettings.put(
                            settingKey,
                            new SecureSettingValue(
                                sourceSecureSettings.getString(settingKey),
                                sourceSecureSettings.getSHA256Digest(settingKey)
                            )
                        );
                    }
                }
            }
        }

        return new SecureSettings() {
            @Override
            public boolean isLoaded() {
                return true;
            }

            @Override
            public SecureString getString(String setting) {
                var secureSettingValue = clonedSettings.get(setting);
                return secureSettingValue != null ? secureSettingValue.value().clone() : null;
            }

            @Override
            public Set<String> getSettingNames() {
                return clonedSettings.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new UnsupportedOperationException("A cloned SecureSetting cannot be a file");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return clonedSettings.get(setting).sha256Digest();
            }

            @Override
            public void close() {}

            @Override
            public void writeTo(StreamOutput out) {
                throw new UnsupportedOperationException("A cloned SecureSetting cannot be serialized");
            }
        };
    }

    private record SecureSettingValue(SecureString value, byte[] sha256Digest) {}
}

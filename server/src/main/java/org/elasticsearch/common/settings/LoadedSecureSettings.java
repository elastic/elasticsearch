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

public class LoadedSecureSettings {

    /**
     * Extracts the {@link SecureSettings}` out of the passed in {@link Settings} object. The {@code Setting} argument has to have the
     * {@code SecureSettings} open/available. Normally {@code SecureSettings} are available only under specific callstacks (eg. during node
     * initialization or during a `reload` call). The returned copy can be reused freely as it will never be closed (this is a bit of
     * cheating, but it is necessary in this specific circumstance). Only works for secure settings of type string (not file).
     *
     * @param source               A {@code Settings} object with its {@code SecureSettings} open/available.
     * @param settingsToCopy The list of settings to copy.
     * @return A copy of the {@code SecureSettings} of the passed in {@code Settings} argument.
     */
    public static SecureSettings toLoadedSecureSettings(Settings source, List<Setting<?>> settingsToCopy) throws GeneralSecurityException {
        final SecureSettings sourceSecureSettings = Settings.builder().put(source, true).getSecureSettings();
        final Map<String, SecureSettingValue> copiedSettings = new HashMap<>();

        if (sourceSecureSettings != null && settingsToCopy != null) {
            for (final String settingKey : sourceSecureSettings.getSettingNames()) {
                for (final Setting<?> secureSetting : settingsToCopy) {
                    if (secureSetting.match(settingKey)) {
                        copiedSettings.put(
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
                return copiedSettings.get(setting).value();
            }

            @Override
            public Set<String> getSettingNames() {
                return copiedSettings.keySet();
            }

            @Override
            public InputStream getFile(String setting) {
                throw new UnsupportedOperationException("A loaded SecureSetting cannot be a file");
            }

            @Override
            public byte[] getSHA256Digest(String setting) {
                return copiedSettings.get(setting).sha256Digest();
            }

            @Override
            public void close() {}

            @Override
            public void writeTo(StreamOutput out) {
                throw new UnsupportedOperationException("A loaded SecureSetting cannot be serialized");
            }
        };
    }

    private record SecureSettingValue(SecureString value, byte[] sha256Digest) {}
}

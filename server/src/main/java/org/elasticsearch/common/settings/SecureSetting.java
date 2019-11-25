/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.util.ArrayUtils;

import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.Set;

/**
 * A secure setting.
 *
 * This class allows access to settings from the Elasticsearch keystore.
 */
public abstract class SecureSetting<T> extends Setting<T> {

    private static final Set<Property> ALLOWED_PROPERTIES = EnumSet.of(Property.Deprecated, Property.Consistent);

    private static final Property[] FIXED_PROPERTIES = {
        Property.NodeScope
    };

    private SecureSetting(String key, Property... properties) {
        super(key, (String)null, null, ArrayUtils.concat(properties, FIXED_PROPERTIES, Property.class));
        assert assertAllowedProperties(properties);
        KeyStoreWrapper.validateSettingName(key);
    }

    private boolean assertAllowedProperties(Setting.Property... properties) {
        for (Setting.Property property : properties) {
            if (ALLOWED_PROPERTIES.contains(property) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String getDefaultRaw(Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    public T getDefault(Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    String innerGetRaw(final Settings settings) {
        throw new UnsupportedOperationException("secure settings are not strings");
    }

    @Override
    public boolean exists(Settings settings) {
        final SecureSettings secureSettings = settings.getSecureSettings();
        return secureSettings != null && secureSettings.getSettingNames().contains(getKey());
    }

    @Override
    public T get(Settings settings) {
        checkDeprecation(settings);
        final SecureSettings secureSettings = settings.getSecureSettings();
        if (secureSettings == null || secureSettings.getSettingNames().contains(getKey()) == false) {
            if (super.exists(settings)) {
                throw new IllegalArgumentException("Setting [" + getKey() + "] is a secure setting" +
                    " and must be stored inside the Elasticsearch keystore, but was found inside elasticsearch.yml");
            }
            return getFallback(settings);
        }
        try {
            return getSecret(secureSettings);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("failed to read secure setting " + getKey(), e);
        }
    }

    /**
     * Returns the digest of this secure setting's value or {@code null} if the setting is missing (inside the keystore). This method can be
     * called even after the {@code SecureSettings} have been closed, unlike {@code #get(Settings)}. The digest is used to check for changes
     * of the value (by re-reading the {@code SecureSettings}), without actually transmitting the value to compare with.
     */
    public byte[] getSecretDigest(Settings settings) {
        final SecureSettings secureSettings = settings.getSecureSettings();
        if (secureSettings == null || false == secureSettings.getSettingNames().contains(getKey())) {
            return null;
        }
        try {
            return secureSettings.getSHA256Digest(getKey());
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("failed to read secure setting " + getKey(), e);
        }
    }

    /** Returns the secret setting from the keyStoreReader store. */
    abstract T getSecret(SecureSettings secureSettings) throws GeneralSecurityException;

    /** Returns the value from a fallback setting. Returns null if no fallback exists. */
    abstract T getFallback(Settings settings);

    // TODO: override toXContent

    /**
     * Overrides the diff operation to make this a no-op for secure settings as they shouldn't be returned in a diff
     */
    @Override
    public void diff(Settings.Builder builder, Settings source, Settings defaultSettings) {
    }

    /**
     * A setting which contains a sensitive string.
     *
     * This may be any sensitive string, e.g. a username, a password, an auth token, etc.
     */
    public static Setting<SecureString> secureString(String name, Setting<SecureString> fallback,
                                                     Property... properties) {
        return new SecureStringSetting(name, fallback, properties);
    }

    /**
     * A setting which contains a file. Reading the setting opens an input stream to the file.
     *
     * This may be any sensitive file, e.g. a set of credentials normally in plaintext.
     */
    public static Setting<InputStream> secureFile(String name, Setting<InputStream> fallback,
                                                  Property... properties) {
        return new SecureFileSetting(name, fallback, properties);
    }

    private static class SecureStringSetting extends SecureSetting<SecureString> {
        private final Setting<SecureString> fallback;

        private SecureStringSetting(String name, Setting<SecureString> fallback, Property... properties) {
            super(name, properties);
            this.fallback = fallback;
        }

        @Override
        protected SecureString getSecret(SecureSettings secureSettings) throws GeneralSecurityException {
            return secureSettings.getString(getKey());
        }

        @Override
        SecureString getFallback(Settings settings) {
            if (fallback != null) {
                return fallback.get(settings);
            }
            return new SecureString(new char[0]); // this means "setting does not exist"
        }
    }

    private static class SecureFileSetting extends SecureSetting<InputStream> {
        private final Setting<InputStream> fallback;

        private SecureFileSetting(String name, Setting<InputStream> fallback, Property... properties) {
            super(name, properties);
            this.fallback = fallback;
        }

        @Override
        protected InputStream getSecret(SecureSettings secureSettings) throws GeneralSecurityException {
            return secureSettings.getFile(getKey());
        }

        @Override
        InputStream getFallback(Settings settings) {
            if (fallback != null) {
                return fallback.get(settings);
            }
            return null;
        }
    }
}

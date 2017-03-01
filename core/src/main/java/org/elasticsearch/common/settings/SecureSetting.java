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

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.ArrayUtils;


/**
 * A secure setting.
 *
 * This class allows access to settings from the Elasticsearch keystore.
 */
public abstract class SecureSetting<T> extends Setting<T> {
    private static final Set<Property> ALLOWED_PROPERTIES = new HashSet<>(
        Arrays.asList(Property.Deprecated, Property.Shared)
    );

    private static final Property[] FIXED_PROPERTIES = {
        Property.NodeScope
    };

    private static final Property[] LEGACY_PROPERTIES = {
        Property.NodeScope, Property.Deprecated, Property.Filtered
    };

    private SecureSetting(String key, Property... properties) {
        super(key, (String)null, null, ArrayUtils.concat(properties, FIXED_PROPERTIES, Property.class));
        assert assertAllowedProperties(properties);
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
    public String getRaw(Settings settings) {
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
            return getFallback(settings);
        }
        try {
            return getSecret(secureSettings);
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
     * A setting which contains a sensitive string.
     *
     * This may be any sensitive string, e.g. a username, a password, an auth token, etc.
     */
    public static Setting<SecureString> secureString(String name, Setting<SecureString> fallback,
                                                     boolean allowLegacy, Property... properties) {
        final Setting<String> legacy;
        if (allowLegacy) {
            Property[] legacyProperties = ArrayUtils.concat(properties, LEGACY_PROPERTIES, Property.class);
            legacy = Setting.simpleString(name, legacyProperties);
        } else {
            legacy = null;
        }
        return new SecureSetting<SecureString>(name, properties) {
            @Override
            protected SecureString getSecret(SecureSettings secureSettings) throws GeneralSecurityException {
                return secureSettings.getString(getKey());
            }
            @Override
            SecureString getFallback(Settings settings) {
                if (legacy != null && legacy.exists(settings)) {
                    return new SecureString(legacy.get(settings).toCharArray());
                }
                if (fallback != null) {
                    return fallback.get(settings);
                }
                return new SecureString(new char[0]); // this means "setting does not exist"
            }
            @Override
            protected void checkDeprecation(Settings settings) {
                super.checkDeprecation(settings);
                if (legacy != null) {
                    legacy.checkDeprecation(settings);
                }
            }
            @Override
            public boolean exists(Settings settings) {
                // handle legacy, which is internal to this setting
                return super.exists(settings) || legacy != null && legacy.exists(settings);
            }
        };
    }


}

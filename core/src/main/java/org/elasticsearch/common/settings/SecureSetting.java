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


/**
 * A secure setting.
 *
 * This class allows access to settings from the Elasticsearch keystore.
 */
public abstract class SecureSetting<T> extends Setting<T> {
    private static final Set<Property> ALLOWED_PROPERTIES = new HashSet<>(
        Arrays.asList(Property.Deprecated, Property.Shared)
    );

    private SecureSetting(String key, Setting.Property... properties) {
        super(key, (String)null, null, properties);
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
    public T get(Settings settings) {
        checkDeprecation(settings);
        final KeyStoreWrapper keystore = Objects.requireNonNull(settings.getKeyStore());
        if (keystore.getSettings().contains(getKey()) == false) {
            return getFallback(settings);
        }
        try {
            return getSecret(keystore);
        } catch (GeneralSecurityException e) {
            throw new RuntimeException("failed to read secure setting " + getKey(), e);
        }
    }

    /** Returns the secret setting from the keyStoreReader store. */
    abstract T getSecret(KeyStoreWrapper keystore) throws GeneralSecurityException;

    /** Returns the value from a fallback setting. Returns null if no fallback exists. */
    abstract T getFallback(Settings settings);

    // TODO: override toXContent

    /**
     * A setting which contains a sensitive string.
     *
     * This may be any sensitive string, e.g. a username, a password, an auth token, etc.
     */
    public static SecureSetting<SecureString> stringSetting(String name, Setting<String> fallback, Property... properties) {
        return new SecureSetting<SecureString>(name, properties) {
            @Override
            protected SecureString getSecret(KeyStoreWrapper keystore) throws GeneralSecurityException {
                return keystore.getStringSetting(getKey());
            }
            @Override
            SecureString getFallback(Settings settings) {
                if (fallback != null) {
                    return new SecureString(fallback.get(settings).toCharArray());
                }
                return null;
            }
        };
    }


}

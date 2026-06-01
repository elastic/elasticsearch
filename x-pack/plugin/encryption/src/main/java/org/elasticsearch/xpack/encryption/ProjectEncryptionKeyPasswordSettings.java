/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.InMemoryClonedSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Secure settings that hold the password used to wrap the project encryption key (PEK) at rest.
 *
 * <p>Two settings are required to enable cluster-state encryption:
 * <ul>
 *   <li>{@code cluster.state.encryption.password.<id>} — password material for a given id
 *   <li>{@code cluster.state.encryption.active_password_id} — the id whose password is active
 * </ul>
 */
final class ProjectEncryptionKeyPasswordSettings {

    static final String PASSWORD_PREFIX = "cluster.state.encryption.password.";
    static final String ACTIVE_PASSWORD_ID_KEY = "cluster.state.encryption.active_password_id";

    static final Setting.AffixSetting<SecureString> PASSWORD = Setting.prefixKeySetting(
        PASSWORD_PREFIX,
        key -> SecureSetting.secureString(key, null)
    );

    static final Setting<SecureString> ACTIVE_PASSWORD_ID = SecureSetting.secureString(ACTIVE_PASSWORD_ID_KEY, null);

    private ProjectEncryptionKeyPasswordSettings() {}

    static List<Setting<?>> getSettings() {
        return List.of(PASSWORD, ACTIVE_PASSWORD_ID);
    }

    /**
     * Returns the active password id, or {@code null} if {@link #ACTIVE_PASSWORD_ID} is unset.
     */
    @Nullable
    static String getActivePasswordId(Settings settings) {
        try (SecureString value = ACTIVE_PASSWORD_ID.get(settings)) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return value.toString();
        }
    }

    /**
     * Returns the password material for {@code id}, or {@code null} if no matching password is configured.
     * Caller owns the returned {@link SecureString} and must close it.
     */
    @Nullable
    static SecureString getPassword(Settings settings, String id) {
        if (id == null || id.isEmpty()) {
            return null;
        }
        try (SecureString value = PASSWORD.getConcreteSetting(PASSWORD_PREFIX + id).get(settings)) {
            if (value == null || value.isEmpty()) {
                return null;
            }
            return value.clone();
        }
    }

    /**
     * Returns whether a password is configured for {@code id} in {@code settings}.
     */
    static boolean hasPassword(Settings settings, String id) {
        return id != null && id.isEmpty() == false && PASSWORD.getConcreteSetting(PASSWORD_PREFIX + id).exists(settings);
    }

    /**
     * Detaches the encryption-related secure settings from {@code source} so callers can keep using {@link Settings} accessors
     * (e.g. {@link #getPassword}, {@link #getActivePasswordId}) after the original keystore has been closed by the
     * {@link org.elasticsearch.plugins.ReloadablePlugin#reload}.
     */
    static Settings cloneSettings(Settings source) {
        try {
            return Settings.builder().setSecureSettings(InMemoryClonedSecureSettings.cloneSecureSettings(source, getSettings())).build();
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("failed to clone project encryption key secure settings", e);
        }
    }
}

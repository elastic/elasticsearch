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
 * Settings governing the project encryption key (PEK) at rest.
 */
final class ProjectEncryptionKeyPasswordSettings {

    static final String PASSWORD_PREFIX = "cluster.state.encryption.password.";
    static final String ACTIVE_PASSWORD_ID_KEY = "cluster.state.encryption.active_password_id";

    static final Setting.AffixSetting<SecureString> PASSWORD = Setting.prefixKeySetting(
        PASSWORD_PREFIX,
        key -> SecureSetting.secureString(key, null)
    );

    static final Setting<SecureString> ACTIVE_PASSWORD_ID = SecureSetting.secureString(ACTIVE_PASSWORD_ID_KEY, null);

    static final Setting<Boolean> ENCRYPTION_REQUIRED = Setting.boolSetting(
        "cluster.state.encryption.required",
        true,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );

    private ProjectEncryptionKeyPasswordSettings() {}

    static List<Setting<?>> getSettings() {
        return List.of(PASSWORD, ACTIVE_PASSWORD_ID, ENCRYPTION_REQUIRED);
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
     * Returns why {@code settings} don't let a node wrap the key for its persisted cluster state, or {@code null} if they do. Derives a
     * wrapping key, since a configured password can still be unusable, such as one too short for FIPS; that is slow, so callers should
     * cache the result.
     */
    @Nullable
    static String wrapForGatewayFailure(Settings settings) {
        String passwordId = getActivePasswordId(settings);
        if (passwordId == null) {
            return "[" + ACTIVE_PASSWORD_ID_KEY + "] is not configured";
        }
        try (SecureString password = getPassword(settings, passwordId)) {
            if (password == null) {
                return "[" + PASSWORD_PREFIX + passwordId + "] is not configured";
            }
            PasswordBasedEncryption.deriveKek(password.getChars(), PasswordBasedEncryption.generateSalt());
            return null;
        } catch (Exception e) {
            return "[" + PASSWORD_PREFIX + passwordId + "] cannot be used to wrap the key: " + e.getMessage();
        }
    }

    /**
     * Rejects a reload that removes a password this node needs while the cluster uses a key: the active one, which it wraps the key
     * with, or the one its on-disk copy of the key is wrapped with, which it needs to read the key back after a restart.
     */
    static void validateReload(boolean currentCanWrap, Settings current, Settings updated, @Nullable String keyOnDiskPasswordId) {
        if (currentCanWrap) {
            String failure = wrapForGatewayFailure(updated);
            if (failure != null) {
                throw new IllegalArgumentException(
                    "cannot reload cluster state encryption settings: the cluster uses a cluster state encryption key, and with the"
                        + " reloaded settings this node could no longer persist cluster state ("
                        + failure
                        + "); keeping the previous settings"
                );
            }
        }
        if (hasPassword(current, keyOnDiskPasswordId) && hasPassword(updated, keyOnDiskPasswordId) == false) {
            String missing = PASSWORD_PREFIX + keyOnDiskPasswordId;
            throw new IllegalArgumentException(
                "cannot reload cluster state encryption settings: this node's on-disk copy of the cluster state encryption key is wrapped"
                    + " with ["
                    + missing
                    + "], which is missing from the reloaded secure settings; keeping the previous settings. Keep ["
                    + missing
                    + "] until this node has persisted cluster state with the new password"
            );
        }
    }

    /**
     * Returns a {@link Settings} that merges all non-secure entries from {@code settings} with a durable in-memory snapshot of the
     * encryption-related secure settings read from the same source. The returned instance does not hold a reference to the original
     * keystore, so it remains valid after {@link org.elasticsearch.plugins.ReloadablePlugin#reload} closes it.
     */
    static Settings cloneSettings(Settings settings) {
        try {
            return Settings.builder()
                .put(settings, false)
                .setSecureSettings(InMemoryClonedSecureSettings.cloneSecureSettings(settings, getSettings()))
                .build();
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("failed to clone project encryption key secure settings", e);
        }
    }
}

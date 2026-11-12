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

    static boolean canWrapForDisk(Settings settings) {
        return hasPassword(settings, getActivePasswordId(settings));
    }

    /**
     * Rejects a reload that removes a password a node holding the key on disk still needs: the active one, which it wraps the key with,
     * or the one its on-disk copy is wrapped with, which it needs to read the key back after a restart.
     */
    static void validateReload(Settings current, Settings updated, @Nullable String onDiskPasswordId) {
        if (onDiskPasswordId == null) {
            return;
        }
        if (canWrapForDisk(current) && canWrapForDisk(updated) == false) {
            String activePasswordId = getActivePasswordId(updated);
            String missing = activePasswordId == null ? ACTIVE_PASSWORD_ID_KEY : PASSWORD_PREFIX + activePasswordId;
            throw new IllegalArgumentException(
                "cannot reload cluster state encryption settings: this node holds the project encryption key on disk and ["
                    + missing
                    + "] is not configured in the reloaded secure settings, so this node could no longer persist the key to disk;"
                    + " keeping the previous settings until this node restarts. Add the setting to the keystore before restarting this node"
            );
        }
        if (hasPassword(current, onDiskPasswordId) && hasPassword(updated, onDiskPasswordId) == false) {
            String missing = PASSWORD_PREFIX + onDiskPasswordId;
            throw new IllegalArgumentException(
                "cannot reload cluster state encryption settings: this node's on-disk copy of the project encryption key is wrapped with ["
                    + missing
                    + "], which is missing from the reloaded secure settings; keeping the previous settings. Keep ["
                    + missing
                    + "] in the keystore until this node has restarted"
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

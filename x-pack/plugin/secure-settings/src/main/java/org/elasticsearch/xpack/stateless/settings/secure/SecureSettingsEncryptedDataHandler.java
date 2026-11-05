/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Re-encrypts API-set secure settings when the project encryption key rotates, and wipes their values on a destructive reset.
 */
public final class SecureSettingsEncryptedDataHandler implements EncryptedDataHandler<SecureSettingsMetadata> {

    @Override
    public String customName() {
        return SecureSettingsMetadata.TYPE;
    }

    /**
     * Not included until snapshot and restore of secure settings is supported.
     */
    @Override
    public boolean includedInSnapshots() {
        return false;
    }

    @Override
    public boolean hasData(SecureSettingsMetadata current) {
        return current != null && current.settings().values().stream().anyMatch(Objects::nonNull);
    }

    /**
     * A dropped value keeps its name with a {@code null} value, like {@link #onDestructiveReset}, so it never falls back to a keystore
     * value. The custom is therefore never removed, even if every value is dropped.
     */
    @Override
    public SecureSettingsMetadata reEncrypt(SecureSettingsMetadata current, UnaryOperator<EncryptedData> rewrapper) {
        if (current == null || current.settings().isEmpty()) {
            return current;
        }
        Map<String, EncryptedData> rebuilt = new HashMap<>(current.settings().size());
        boolean changed = false;
        for (Map.Entry<String, EncryptedData> entry : current.settings().entrySet()) {
            EncryptedData existing = entry.getValue();
            EncryptedData rewrapped = existing == null ? null : rewrapper.apply(existing);
            rebuilt.put(entry.getKey(), rewrapped);
            changed |= rewrapped != existing;
        }
        return changed ? new SecureSettingsMetadata(rebuilt) : current;
    }

    /**
     * Keeps every name with a {@code null} value, so a consumer gets an error asking for the value to be provisioned again instead of
     * silently falling back to a keystore value for the same name.
     */
    @Override
    public SecureSettingsMetadata onDestructiveReset(SecureSettingsMetadata current) {
        if (current == null || current.settings().isEmpty()) {
            return current;
        }
        Map<String, EncryptedData> wiped = new HashMap<>(current.settings().size());
        for (String name : current.settings().keySet()) {
            wiped.put(name, null);
        }
        return new SecureSettingsMetadata(wiped);
    }
}

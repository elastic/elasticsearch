/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

/**
 * The operational state of the cluster-state encryption service on the local node.
 */
public enum EncryptionServiceState {

    /**
     * Encryption is fully operational, or the service is initializing and a key will become available soon (e.g., first key install
     * pending). A {@link EncryptionKeyNotYetAvailableException} from this state is transient.
     */
    READY("ready"),

    /**
     * No {@code cluster.state.encryption.active_password_id} is configured. Encryption cannot be performed until the operator provisions
     * the password settings.
     */
    DISABLED("disabled"),

    /**
     * An active password id is configured, but no matching {@code cluster.state.encryption.password.<id>} secure setting is present. The
     * operator must provision the matching password material.
     */
    UNAVAILABLE_MISSING_PASSWORD("unavailable: missing_password"),

    /**
     * The matching password setting is present, but it fails to decrypt the persisted project encryption key (wrong password or
     * corrupted key). The operator must correct the password or use the reset API to start over.
     */
    UNAVAILABLE_DECRYPTION_FAILED("unavailable: decryption_failed");

    private final String displayValue;

    EncryptionServiceState(String displayValue) {
        this.displayValue = displayValue;
    }

    public String displayValue() {
        return displayValue;
    }

    /**
     * Returns a short, operator-facing hint describing how to recover from this state.
     * Returns an empty string for {@link #READY} — callers may check with {@link String#isEmpty()}.
     */
    public String actionableHint() {
        return switch (this) {
            case READY -> "";
            case DISABLED -> "Configure cluster.state.encryption.active_password_id and the matching"
                + " cluster.state.encryption.password.<id> in the keystore.";
            case UNAVAILABLE_MISSING_PASSWORD -> "Provision the matching cluster.state.encryption.password.<id> in the keystore"
                + " and call POST /_nodes/reload_secure_settings (stateful).";
            case UNAVAILABLE_DECRYPTION_FAILED -> "Verify that cluster.state.encryption.password.<id> is correct."
                + " If the password is unrecoverable, run POST /_encryption/_reset?accept_data_loss=true.";
        };
    }
}

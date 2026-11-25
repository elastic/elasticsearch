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
     * The elected master could not decrypt its persisted project encryption key and published cluster state without it. It gets the key
     * back from its own disk once it has the password, or another master-eligible node that can read its copy is elected; the reset API
     * is the last resort if no node can.
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
            case UNAVAILABLE_DECRYPTION_FAILED -> "The elected master holds no encryption key and cannot read the key on its disk."
                + " Check the cluster_state_encryption health indicator for details.";
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Provides symmetric encrypt/decrypt operations.
 *
 * <p>Callers never handle raw key material. The returned {@link EncryptedData} is self-describing, carrying the key ID alongside the
 * encrypted payload so that features can store both.
 *
 * <p>Features that store encrypted data must register an {@code EncryptedDataHandler} so that their data can be re-encrypted under
 * a new key when the project encryption key rotates. Registration is done via the {@code EncryptedDataHandlerProvider} SPI.
 *
 * <p>{@link #encrypt} throws {@link org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException} when the service is
 * in a permanent degraded state (missing password, decryption failure) and
 * {@link org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException} when the key is transiently unavailable.
 * Callers that need to tolerate unavailability (opt-out via {@code cluster.state.encryption.required: false}) should check
 * {@link #isEncryptionRequired()} and catch those exceptions to fall back to plaintext with a warning.
 */
public interface EncryptionService {

    /**
     * Encrypts the given bytes using the current active key.
     *
     * @param bytes the plaintext to encrypt; may be empty but not null
     * @return an {@link EncryptedData} carrier containing the key ID and the encrypted payload
     * @throws EncryptionServiceUnavailableException if the service is in a permanent degraded state
     * @throws EncryptionKeyNotYetAvailableException if the key is transiently unavailable
     */
    EncryptedData encrypt(byte[] bytes);

    /**
     * Decrypts data previously produced by {@link #encrypt(byte[])}.
     *
     * @param encryptedData the carrier returned by a prior {@link #encrypt} call
     * @return the original plaintext bytes
     * @throws EncryptionServiceUnavailableException if the service is in a permanent degraded state
     * @throws EncryptionKeyNotYetAvailableException if the key named by {@link EncryptedData#keyId()} is transiently unavailable
     */
    byte[] decrypt(EncryptedData encryptedData);

    /**
     * Returns {@code true} if callers must refuse to store secrets when encryption is unavailable.
     * Returns {@code false} if callers may store secrets in plaintext (with a warning) as a fallback.
     *
     * <p>Defaults to {@code true}. Set {@code cluster.state.encryption.required: false} in node settings to opt out.
     */
    default boolean isEncryptionRequired() {
        return true;
    }

    /**
     * Process-wide holder for the single {@link EncryptionService} instance.
     *
     * <p>The encryption plugin populates this once from its {@code createComponents}. It exists for {@code createComponents}-time
     * consumers in other plugins (e.g. ES|QL data sources) that build their own components before Guice wiring
     * is available and therefore cannot have the service injected. Plugin load ordering
     * guarantees the encryption plugin's {@code createComponents} runs before any consumer's.
     */
    final class Holder {

        private static final AtomicReference<EncryptionService> INSTANCE = new AtomicReference<>();

        private Holder() {}

        /**
         * Returns the registered {@link EncryptionService}.
         *
         * @throws IllegalStateException if no instance has been set yet
         */
        public static EncryptionService get() {
            EncryptionService service = INSTANCE.get();
            if (service == null) {
                throw new IllegalStateException("encryption service is not constructed yet");
            }
            return service;
        }

        /**
         * Registers the single {@link EncryptionService} instance.
         */
        public static void set(EncryptionService service) {
            INSTANCE.set(Objects.requireNonNull(service, "encryption service must not be null"));
        }
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption.WrappedKey;

/** Shared no-op {@link PekEncryption} for use in unit tests. */
class TestPekEncryption {

    /** Password id reported by {@link #NO_OP} and baked into its {@link WrappedKey} results. */
    static final String PASSWORD_ID = "v1";

    /**
     * Pass-through encryption: {@link PekEncryption#wrap} returns a defensive copy of the plaintext keyed under
     * {@link #PASSWORD_ID}; {@link PekEncryption#unwrap} returns a defensive copy of whatever bytes it receives.
     * Suitable for any unit test that needs a valid {@link PekEncryption} but does not exercise the cryptographic path.
     */
    static final PekEncryption NO_OP = new PekEncryption() {
        @Override
        public String activePasswordId() {
            return PASSWORD_ID;
        }

        @Override
        public WrappedKey wrap(byte[] plaintextPek) {
            return new WrappedKey(PASSWORD_ID, plaintextPek.clone());
        }

        @Override
        public byte[] unwrap(byte[] wrappedPek, String passwordId) {
            return wrappedPek.clone();
        }
    };

    private TestPekEncryption() {}
}

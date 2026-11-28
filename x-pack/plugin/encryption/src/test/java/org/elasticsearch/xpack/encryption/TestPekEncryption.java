/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.GatewayKeys;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption.WrappedKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;

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

    /** Returns {@code delegate} with its own {@link GatewayKeys}, like one node's {@link PasswordPekEncryption}. */
    static PekEncryption onNode(PekEncryption delegate, GatewayKeys gatewayKeys) {
        return new PekEncryption() {
            @Override
            public String activePasswordId() {
                return delegate.activePasswordId();
            }

            @Override
            public WrappedKey wrap(byte[] plaintextPek) {
                return delegate.wrap(plaintextPek);
            }

            @Override
            public byte[] unwrap(byte[] wrappedPek, String passwordId) {
                return delegate.unwrap(wrappedPek, passwordId);
            }

            @Override
            public void onGatewayWrite(Exception wrapFailure, boolean keysWritten) {
                delegate.onGatewayWrite(wrapFailure, keysWritten);
            }

            @Override
            public GatewayKeys gatewayKeys() {
                return gatewayKeys;
            }
        };
    }

    /** Writes {@code metadata} the way a node persists it, which records the wrapped keys in that node's {@link GatewayKeys}. */
    static void persist(ProjectEncryptionKeyMetadata metadata) {
        ToXContent.Params params = new ToXContent.MapParams(Map.of(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY));
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            Iterator<? extends ToXContent> chunks = metadata.toXContentChunked(params);
            while (chunks.hasNext()) {
                chunks.next().toXContent(builder, params);
            }
            builder.endObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TestPekEncryption() {}
}

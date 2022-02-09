/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;

import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.SecretKey;

/**
 * Container class for a {@code SecretKey} with a unique identifier, and a 4-byte wide {@code Integer} nonce, that can be used for a
 * single encryption operation. Use {@link #createSingleUseKeySupplier(CheckedSupplier)} to obtain a {@code Supplier} that returns
 * a new {@link SingleUseKey} instance on every invocation. The number of unique {@code SecretKey}s (and their associated identifiers)
 * generated is minimized and, at the same time, ensuring that a given {@code nonce} is not reused with the same key.
 */
final class SingleUseKey {
    private static final Logger logger = LogManager.getLogger(SingleUseKey.class);
    static final int MIN_NONCE = Integer.MIN_VALUE;
    static final int MAX_NONCE = Integer.MAX_VALUE;
    private static final int MAX_ATTEMPTS = 9;
    private static final SingleUseKey EXPIRED_KEY = new SingleUseKey(null, null, MAX_NONCE);

    private final BytesReference keyId;
    private final SecretKey key;
    private final int nonce;

    // for tests use only!
    SingleUseKey(BytesReference KeyId, SecretKey Key, int nonce) {
        this.keyId = KeyId;
        this.key = Key;
        this.nonce = nonce;
    }

    public BytesReference getKeyId() {
        return keyId;
    }

    public SecretKey getKey() {
        return key;
    }

    public int getNonce() {
        return nonce;
    }

    /**
     * Returns a {@code CheckedSupplier} of {@code SingleUseKey}s so that no two instances contain the same key and nonce pair.
     * The current implementation increments the {@code nonce} while keeping the key constant, until the {@code nonce} space
     * is exhausted, at which moment a new key is generated and the {@code nonce} is reset back.
     *
     * @param keyGenerator supplier for the key and the key id
     */
    static <T extends Exception> CheckedSupplier<SingleUseKey, T> createSingleUseKeySupplier(
        CheckedSupplier<Tuple<BytesReference, SecretKey>, T> keyGenerator
    ) {
        final AtomicReference<SingleUseKey> keyCurrentlyInUse = new AtomicReference<>(EXPIRED_KEY);
        return internalSingleUseKeySupplier(keyGenerator, keyCurrentlyInUse);
    }

    // for tests use only, the {@code keyCurrentlyInUse} must not be exposed to caller code
    static <T extends Exception> CheckedSupplier<SingleUseKey, T> internalSingleUseKeySupplier(
        CheckedSupplier<Tuple<BytesReference, SecretKey>, T> keyGenerator,
        AtomicReference<SingleUseKey> keyCurrentlyInUse
    ) {
        final Object lock = new Object();
        return () -> {
            for (int attemptNo = 0; attemptNo < MAX_ATTEMPTS; attemptNo++) {
                final SingleUseKey nonceAndKey = keyCurrentlyInUse.getAndUpdate(
                    prev -> prev.nonce < MAX_NONCE ? new SingleUseKey(prev.keyId, prev.key, prev.nonce + 1) : EXPIRED_KEY
                );
                if (nonceAndKey.nonce < MAX_NONCE) {
                    // this is the commonly used code path, where just the nonce is incremented
                    logger.trace(
                        () -> new ParameterizedMessage("Key with id [{}] reused with nonce [{}]", nonceAndKey.keyId, nonceAndKey.nonce)
                    );
                    return nonceAndKey;
                } else {
                    // this is the infrequent code path, where a new key is generated and the nonce is reset back
                    logger.trace(
                        () -> new ParameterizedMessage("Try to generate a new key to replace the key with id [{}]", nonceAndKey.keyId)
                    );
                    synchronized (lock) {
                        if (keyCurrentlyInUse.get().nonce == MAX_NONCE) {
                            final Tuple<BytesReference, SecretKey> newKey = keyGenerator.get();
                            logger.debug(() -> new ParameterizedMessage("New key with id [{}] has been generated", newKey.v1()));
                            keyCurrentlyInUse.set(new SingleUseKey(newKey.v1(), newKey.v2(), MIN_NONCE));
                        }
                    }
                }
            }
            throw new IllegalStateException("Failure to generate new key");
        };
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Component-bound wrapper around the {@link EncryptedDataHandler}s contributed via the {@link EncryptedDataHandlerProvider} SPI. Built
 * once by {@link EncryptionPlugin} and injected into {@link TransportEncryptionResetAction} for dropping handler-owned customs during a
 * destructive reset. Wrapping the list keeps Guice's component graph explicit.
 *
 * <p>The node's instance is also published to a static slot so that {@link EncryptingSnapshotGlobalStateTransformer}, which is
 * instantiated through the {@code SnapshotGlobalStateTransformer} service-provider mechanism (no constructor injection available),
 * can resolve it lazily at transform time.
 */
public record EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {

    private static final AtomicReference<EncryptedDataHandlerRegistry> INSTANCE = new AtomicReference<>();

    public EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {
        this.handlers = List.copyOf(handlers);
    }

    /**
     * @return the node's registry, published by {@link EncryptionPlugin}'s {@code createComponents}.
     * @throws IllegalStateException if the encryption plugin hasn't built the registry yet.
     */
    public static EncryptedDataHandlerRegistry getInstance() {
        EncryptedDataHandlerRegistry registry = INSTANCE.get();
        if (registry == null) {
            throw new IllegalStateException("EncryptedDataHandlerRegistry is not constructed yet");
        }
        return registry;
    }

    static void setInstance(EncryptedDataHandlerRegistry registry) {
        INSTANCE.set(registry);
    }

    /**
     * Clears the static slot so a later {@link #setInstance} starts from a clean state rather than a registry left behind by a
     * previously constructed plugin instance (e.g. a prior node in the same test JVM). Called from {@link EncryptionPlugin}'s
     * constructor, mirroring {@code EncryptionServiceRegistry#reset}.
     */
    static void reset() {
        INSTANCE.set(null);
    }
}

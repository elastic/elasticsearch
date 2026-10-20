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
 * Holds the node's single {@link EncryptionService} instance so that plugins extending the encryption SPI can obtain it from their own
 * {@code createComponents}, before Guice wiring is available and an injected dependency would be possible.
 *
 * <p>Living in the SPI jar means the encryption plugin and every extending plugin resolve this class through the same class loader,
 * so they all see the same static slot.
 *
 * <p>Consumers wired through Guice should prefer having the {@link EncryptionService} injected (it is published as a
 * {@code PluginComponentBinding} from the same {@code createComponents} that populates this registry) rather than reading this
 * static slot directly.
 */
public final class EncryptionServiceRegistry {

    private static final AtomicReference<EncryptionService> instance = new AtomicReference<>();

    /**
     * @return the node's {@link EncryptionService}, registered by the encryption plugin's {@code createComponents}.
     * @throws IllegalStateException if the encryption plugin hasn't wired the service yet.
     */
    public static EncryptionService getEncryptionService() {
        EncryptionService service = instance.get();
        if (service == null) {
            throw new IllegalStateException("EncryptionService is not constructed yet");
        }
        return service;
    }

    /**
     * Registers the node's {@link EncryptionService}. Called by the encryption plugin from {@code createComponents}
     */
    public static void setEncryptionService(EncryptionService service) {
        instance.set(Objects.requireNonNull(service, "encryptionService"));
    }

    /**
     * Clears the registry so a later {@link #setEncryptionService} call starts from a clean state instead of overwriting (or being
     * mistaken for) a service left behind by a previously constructed plugin instance. The encryption plugin calls this from its
     * constructor, which runs again for every node built in the same JVM (e.g. multi-node integration tests).
     */
    public static void reset() {
        instance.set(null);
    }

    private EncryptionServiceRegistry() {
        throw new IllegalAccessError("not allowed!");
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Component-bound wrapper around the {@link EncryptedDataHandler}s contributed via the {@link EncryptedDataHandlerProvider} SPI. Built
 * once by {@link EncryptionPlugin} and injected into {@link TransportEncryptionResetAction} for dropping handler-owned customs during a
 * destructive reset. Wrapping the list keeps Guice's component graph explicit.
 *
 * <p>The class also maintains a static slot ({@link #INSTANCE}) so that SPI-instantiated components (e.g.
 * {@link EncryptingSnapshotGlobalStateTransformer}) that are created outside Guice can obtain the handlers
 * lazily without requiring a separate registry in the SPI jar.
 */
public record EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {

    static final AtomicReference<EncryptedDataHandlerRegistry> INSTANCE = new AtomicReference<>();

    public EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {
        this.handlers = List.copyOf(handlers);
    }

    /**
     * Registers this registry in the static slot so SPI-instantiated components (e.g.
     * {@link EncryptingSnapshotGlobalStateTransformer}) that are created outside Guice can resolve it lazily.
     * Called by {@link EncryptionPlugin} from {@code createComponents}.
     */
    static void setInstance(EncryptedDataHandlerRegistry registry) {
        INSTANCE.set(registry);
    }

    /**
     * Returns the singleton instance registered by the encryption plugin's {@code createComponents}, or {@code null} if the plugin has
     * not wired one. The SPI transformer can be discovered without the plugin being installed (e.g. when plugins share a classpath in
     * internal cluster tests), so callers must treat {@code null} as "no encrypted data handlers".
     */
    @Nullable
    static EncryptedDataHandlerRegistry getInstance() {
        return INSTANCE.get();
    }
}

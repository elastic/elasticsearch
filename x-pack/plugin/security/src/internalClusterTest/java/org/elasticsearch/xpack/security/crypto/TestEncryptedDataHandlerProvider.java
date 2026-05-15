/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.encryption.EncryptedDataHandler;
import org.elasticsearch.encryption.EncryptedDataHandlerProvider;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Discovered by {@code PluginsService.loadServiceProviders} via the {@code META-INF/services} entry in this test module's resources.
 * Registers a no-op {@link TestSpiHandler} so {@link EncryptedDataHandlerProviderSpiIT} can verify the SPI wiring works end-to-end.
 */
public class TestEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    /** Incremented once per {@code reEncrypt} invocation across all instances. Tests read it to verify the SPI handler ran. */
    public static final AtomicInteger INVOCATIONS = new AtomicInteger();

    @Override
    public Collection<EncryptedDataHandler> getHandlers() {
        return List.of(new TestSpiHandler());
    }

    static final class TestSpiHandler implements EncryptedDataHandler {
        @Override
        public void reEncrypt(String activeKeyId, ActionListener<Void> listener) {
            INVOCATIONS.incrementAndGet();
            listener.onResponse(null);
        }
    }
}

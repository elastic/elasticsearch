/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.encryption.spi.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandler;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandlerProvider;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Discovered via {@code ExtensiblePlugin.loadExtensions} by the security plugin. Registers a single no-op handler that increments
 * an in-JVM counter so the IT can verify the SPI is wired end-to-end.
 */
public class TestEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    /** Bumped once per {@code reEncrypt} invocation; readable from the cluster JVM via the test plugin's REST handler. */
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

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption.spi.test;

import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

/**
 * Discovered via {@code ExtensiblePlugin.loadExtensions} by the encryption plugin. Registers a {@link TestSpiHandler} that owns a
 * {@link TestEncryptedBlob} project custom. {@link #getHandlers} bumps an in-JVM counter, readable from the cluster JVM via the test
 * plugin's REST handler, so an out-of-process test can observe that the provider was discovered and its handlers registered.
 */
public class TestEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    /** Bumped once per {@code getHandlers} invocation; readable from the cluster JVM via the test plugin's REST handler. */
    public static final AtomicInteger INVOCATIONS = new AtomicInteger();

    @Override
    public Collection<EncryptedDataHandler<?>> getHandlers() {
        INVOCATIONS.incrementAndGet();
        return List.of(new TestSpiHandler());
    }

    static final class TestSpiHandler implements EncryptedDataHandler<TestEncryptedBlob> {

        @Override
        public String customName() {
            return TestEncryptedBlob.TYPE;
        }

        @Override
        public TestEncryptedBlob reEncrypt(TestEncryptedBlob current, UnaryOperator<EncryptedData> reEncrypt) {
            EncryptedData rewrapped = reEncrypt.apply(current.blob());
            return rewrapped == current.blob() ? current : new TestEncryptedBlob(rewrapped);
        }
    }
}

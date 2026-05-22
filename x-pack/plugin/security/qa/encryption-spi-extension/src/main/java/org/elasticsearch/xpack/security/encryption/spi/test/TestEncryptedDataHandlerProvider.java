/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.encryption.spi.test;

import org.elasticsearch.xpack.security.spi.encryption.EncryptedData;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandler;
import org.elasticsearch.xpack.security.spi.encryption.EncryptedDataHandlerProvider;
import org.elasticsearch.xpack.security.spi.encryption.EncryptionService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Discovered via {@code ExtensiblePlugin.loadExtensions} by the security plugin. Registers a {@link TestSpiHandler} that owns a
 * {@link TestEncryptedBlob} project custom; whenever the coordinator schedules re-encryption the handler decrypts the blob with the
 * previous key and re-encrypts under the active key.
 */
public class TestEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    /** Bumped once per {@code reEncrypt} invocation; readable from the cluster JVM via the test plugin's REST handler. */
    public static final AtomicInteger INVOCATIONS = new AtomicInteger();

    @Override
    public Collection<EncryptedDataHandler<?>> getHandlers() {
        return List.of(new TestSpiHandler());
    }

    static final class TestSpiHandler implements EncryptedDataHandler<TestEncryptedBlob> {

        @Override
        public String customName() {
            return TestEncryptedBlob.TYPE;
        }

        @Override
        public TestEncryptedBlob reEncrypt(TestEncryptedBlob current, EncryptionService encryptionService, String activeKeyId) {
            INVOCATIONS.incrementAndGet();
            if (current == null) {
                return null;
            }
            EncryptedData existing = current.blob();
            if (existing.keyId().equals(activeKeyId)) {
                return current;
            }
            byte[] plaintext = encryptionService.decrypt(existing);
            return new TestEncryptedBlob(encryptionService.encrypt(plaintext));
        }
    }
}

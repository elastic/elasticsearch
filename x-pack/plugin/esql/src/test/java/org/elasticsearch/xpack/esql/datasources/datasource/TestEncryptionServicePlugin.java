/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.datasource;

import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceRegistry;

import java.util.Collection;
import java.util.List;

/**
 * Test-only {@link EncryptionService} binding so the CRUD ITs exercise the encryption path
 * without provisioning a real {@code PrimaryEncryptionKey}. Encrypt wraps the input as-is under
 * a fixed test key id; decrypt unwraps. Not cryptographically meaningful — purely a stand-in
 * for the binding the security plugin would provide when {@code xpack.security} is enabled.
 */
public class TestEncryptionServicePlugin extends Plugin {

    public static final String TEST_KEY_ID = "test-key";

    public TestEncryptionServicePlugin() {
        EncryptionServiceRegistry.reset();
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        EncryptionService svc = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData(TEST_KEY_ID, bytes);
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };
        EncryptionServiceRegistry.setEncryptionService(svc);
        return List.of(new PluginComponentBinding<>(EncryptionService.class, svc));
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.esql.qa.action;

import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.Collection;
import java.util.List;

/** Binds a stub {@link EncryptionService} so the data-source CRUD action can be constructed under non-optional injection. */
public class TestEncryptionServicePlugin extends Plugin {
    @Override
    public Collection<?> createComponents(PluginServices services) {
        EncryptionService svc = new EncryptionService() {
            @Override
            public EncryptedData encrypt(byte[] bytes) {
                return new EncryptedData("test-key", bytes);
            }

            @Override
            public byte[] decrypt(EncryptedData encryptedData) {
                return encryptedData.payload();
            }
        };
        return List.of(new PluginComponentBinding<>(EncryptionService.class, svc));
    }
}

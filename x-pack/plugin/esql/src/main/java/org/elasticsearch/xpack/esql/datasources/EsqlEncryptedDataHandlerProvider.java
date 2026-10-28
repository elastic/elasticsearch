/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandler;
import org.elasticsearch.xpack.encryption.spi.EncryptedDataHandlerProvider;

import java.util.Collection;
import java.util.List;

/**
 * Contributes the {@link DataSourceEncryptedDataHandler} to the project encryption key rotation coordinator.
 * Discovered by the encryption plugin via the {@code META-INF/services} SPI entry for
 * {@link EncryptedDataHandlerProvider}.
 */
public final class EsqlEncryptedDataHandlerProvider implements EncryptedDataHandlerProvider {

    @Override
    public Collection<EncryptedDataHandler<?>> getHandlers() {
        return List.of(new DataSourceEncryptedDataHandler());
    }
}

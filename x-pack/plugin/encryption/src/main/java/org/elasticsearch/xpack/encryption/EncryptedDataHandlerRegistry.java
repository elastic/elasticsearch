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

/**
 * Component-bound wrapper around the {@link EncryptedDataHandler}s contributed via the {@link EncryptedDataHandlerProvider} SPI. Built
 * once by {@link EncryptionPlugin} and injected into {@link TransportEncryptionResetAction} for dropping handler-owned customs during a
 * destructive reset. Wrapping the list keeps Guice's component graph explicit.
 */
public record EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {

    public EncryptedDataHandlerRegistry(List<EncryptedDataHandler<?>> handlers) {
        this.handlers = List.copyOf(handlers);
    }
}

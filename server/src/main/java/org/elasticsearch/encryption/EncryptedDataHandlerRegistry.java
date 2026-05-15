/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.encryption;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Shared list of {@link EncryptedDataHandler}s that own data encrypted under the primary encryption key.
 */
public final class EncryptedDataHandlerRegistry {

    private final List<EncryptedDataHandler> handlers = new CopyOnWriteArrayList<>();

    public void register(EncryptedDataHandler handler) {
        handlers.add(handler);
    }

    public List<EncryptedDataHandler> handlers() {
        return handlers;
    }
}

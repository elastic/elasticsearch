/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import org.elasticsearch.core.AbstractRefCounted;

import org.elasticsearch.core.Releasable;

import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;

class AzureConnectionProviderReference extends AbstractRefCounted implements Releasable {

    private final ConnectionProvider connectionProvider;

    AzureConnectionProviderReference(ConnectionProvider connectionProvider) {
        this.connectionProvider = connectionProvider;
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        // same as what we have today in `AzureClientProvider`
        connectionProvider.disposeLater().block(Duration.ofSeconds(5));
    }
}

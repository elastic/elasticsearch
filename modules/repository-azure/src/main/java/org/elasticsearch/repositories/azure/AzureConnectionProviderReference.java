/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure;

import reactor.netty.resources.ConnectionProvider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;

/// Handles the disposal of the wrapped [ConnectionProvider] using reference counting.
class AzureConnectionProviderReference extends AbstractRefCounted implements Releasable {
    private final ConnectionProvider connectionProvider;
    private final ActionListener<Void> disposalListener;

    AzureConnectionProviderReference(ConnectionProvider connectionProvider, ActionListener<Void> disposalListener) {
        this.connectionProvider = connectionProvider;
        this.disposalListener = disposalListener;
    }

    public ConnectionProvider connectionProvider() {
        return connectionProvider;
    }

    @Override
    public void close() {
        decRef();
    }

    @Override
    protected void closeInternal() {
        connectionProvider.disposeLater()
            .subscribe(
                ignored -> {},
                t -> disposalListener.onFailure(t instanceof Exception e ? e : new RuntimeException(t)),
                () -> disposalListener.onResponse(null)
            );
    }
}

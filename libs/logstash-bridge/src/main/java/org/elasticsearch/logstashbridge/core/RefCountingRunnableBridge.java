/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.core;

import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.logstashbridge.StableBridgeAPI;

public class RefCountingRunnableBridge extends StableBridgeAPI.ProxyInternal<RefCountingRunnable> {

    private RefCountingRunnableBridge(final RefCountingRunnable delegate) {
        super(delegate);
    }

    public RefCountingRunnableBridge(final Runnable delegate) {
        super(new RefCountingRunnable(delegate));
    }

    public void close() {
        toInternal().close();
    }

    public ReleasableBridge acquire() {
        return new ReleasableBridge.ProxyInternal(toInternal().acquire());
    }

    @Override
    public RefCountingRunnable toInternal() {
        return this.internalDelegate;
    }
}

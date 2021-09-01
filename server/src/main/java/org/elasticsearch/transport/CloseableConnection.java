/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CompletableContext;


/**
 * Abstract Transport.Connection that provides common close logic.
 */
public abstract class CloseableConnection implements Transport.Connection {

    private final CompletableContext<Void> closeContext = new CompletableContext<>();

    @Override
    public void addCloseListener(ActionListener<Void> listener) {
        closeContext.addListener(ActionListener.toBiConsumer(listener));
    }

    @Override
    public boolean isClosed() {
        return closeContext.isDone();
    }

    @Override
    public void close() {
        // This method is safe to call multiple times as the close context will provide concurrency
        // protection and only be completed once. The attached listeners will only be notified once.
        closeContext.complete(null);
    }
}

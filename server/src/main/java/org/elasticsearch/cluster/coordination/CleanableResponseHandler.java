/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;

/**
 * Combines an ActionListenerResponseHandler with an ActionListener.runAfter action, but with an explicit type so that tests that simulate
 * reboots can release resources without invoking the listener.
 */
public class CleanableResponseHandler<T extends TransportResponse> extends ActionListenerResponseHandler<T> {
    private final Runnable cleanup;

    public CleanableResponseHandler(ActionListener<? super T> listener, Writeable.Reader<T> reader, String executor, Runnable cleanup) {
        super(ActionListener.runAfter(listener, cleanup), reader, executor);
        this.cleanup = cleanup;
    }

    public void runCleanup() {
        assert ThreadPool.assertCurrentThreadPool(); // should only be called from tests which simulate abrupt node restarts
        cleanup.run();
    }
}

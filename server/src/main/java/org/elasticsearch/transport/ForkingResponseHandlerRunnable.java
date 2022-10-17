/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

import static org.elasticsearch.core.Strings.format;

/**
 * {@link AbstractRunnable} implementation for completing a forking response handler, overriding any threadpool queue length limit and
 * handling shutdown-related rejections by completing the handler without forking.
 */
abstract class ForkingResponseHandlerRunnable extends AbstractRunnable {

    private static final Logger logger = LogManager.getLogger(ForkingResponseHandlerRunnable.class);

    private final TransportResponseHandler<?> handler;

    @Nullable
    private final TransportException transportException;

    ForkingResponseHandlerRunnable(TransportResponseHandler<?> handler, @Nullable TransportException transportException) {
        assert handler.executor().equals(ThreadPool.Names.SAME) == false : "forking handler required, but got " + handler;
        this.handler = handler;
        this.transportException = transportException;
    }

    @Override
    protected abstract void doRun(); // no 'throws Exception' here

    @Override
    public boolean isForceExecution() {
        // we must complete every pending listener
        return true;
    }

    @Override
    public void onRejection(Exception e) {
        // force-executed tasks are only rejected on shutdown, but we should have enqueued the completion of every handler before shutting
        // down any thread pools so this indicates a bug
        assert false : e;

        // we must complete every pending listener, and we can't fork to the target threadpool because we're shutting down, so just complete
        // it on this thread.
        final TransportException exceptionToDeliver;
        if (transportException == null) {
            exceptionToDeliver = new RemoteTransportException(e.getMessage(), e);
        } else {
            exceptionToDeliver = transportException;
            exceptionToDeliver.addSuppressed(e);
        }
        try {
            handler.handleException(exceptionToDeliver);
        } catch (Exception e2) {
            exceptionToDeliver.addSuppressed(e2);
            logger.error(
                () -> format(
                    "%s [%s]",
                    transportException == null ? "failed to handle rejection of response" : "failed to handle rejection of error response",
                    handler
                ),
                exceptionToDeliver
            );
        }
    }

    @Override
    public void onFailure(Exception e) {
        assert false : e; // delivering the response shouldn't throw anything
        logger.error(
            () -> format(
                "%s [%s]",
                transportException == null ? "failed to handle rejection of response" : "failed to handle rejection of error response",
                handler
            ),
            e
        );
    }
}

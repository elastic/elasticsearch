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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.threadpool.ThreadPool;

abstract class ForkingResponseRunnable extends AbstractRunnable {

    private static final Logger logger = LogManager.getLogger(ForkingResponseRunnable.class);

    private final TransportResponseHandler<?> handler;

    @Nullable
    private final TransportException transportException;

    ForkingResponseRunnable(TransportResponseHandler<?> handler, @Nullable TransportException transportException) {
        assert handler.executor().equals(ThreadPool.Names.SAME) == false : "forking handler required, but got " + handler;
        this.handler = handler;
        this.transportException = transportException;
    }

    @Override
    public boolean isForceExecution() {
        // we must complete every pending listener
        return true;
    }

    @Override
    public void onRejection(Exception e) {
        // even force-executed tasks are rejected on shutdown
        assert e instanceof EsRejectedExecutionException esRejectedExecutionException && esRejectedExecutionException.isExecutorShutdown()
            : e;

        // we must complete every pending listener, but we can't fork to the target threadpool because we're shutting
        // down, so just complete it on this thread.
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
            e.addSuppressed(e2);
            logger.error(
                () -> new ParameterizedMessage(
                    "failed to handle rejection of {}response [{}]",
                    transportException == null ? "" : "error ",
                    handler
                ),
                e
            );
        }
    }

    @Override
    public void onFailure(Exception e) {
        assert false : e; // delivering the response shouldn't throw anything
        logger.error(
            () -> new ParameterizedMessage("failed to handle {}response [{}]", transportException == null ? "" : "error ", handler),
            e
        );
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.concurrent.Executor;

/**
 * Base class for action listeners that wrap another action listener and dispatch its completion to an executor.
 */
public abstract class AbstractThreadedActionListener<Response> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(AbstractThreadedActionListener.class);

    protected final Executor executor;
    protected final ActionListener<Response> delegate;
    protected final boolean forceExecution;

    protected AbstractThreadedActionListener(Executor executor, boolean forceExecution, ActionListener<Response> delegate) {
        this.forceExecution = forceExecution;
        this.executor = executor;
        this.delegate = delegate;
    }

    @Override
    public final void onFailure(final Exception e) {
        executor.execute(new AbstractRunnable() {
            @Override
            public boolean isForceExecution() {
                return forceExecution;
            }

            @Override
            protected void doRun() {
                delegate.onFailure(e);
            }

            @Override
            public void onRejection(Exception rejectionException) {
                rejectionException.addSuppressed(e);
                try {
                    delegate.onFailure(rejectionException);
                } catch (Exception doubleFailure) {
                    rejectionException.addSuppressed(doubleFailure);
                    onFailure(rejectionException);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(() -> "failed to execute failure callback on [" + AbstractThreadedActionListener.this + "]", e);
                assert false : e;
            }

            @Override
            public String toString() {
                return AbstractThreadedActionListener.this + "/onFailure";
            }
        });
    }

    @Override
    public final String toString() {
        return getClass().getSimpleName() + "[" + executor + "/" + delegate + "]";
    }
}

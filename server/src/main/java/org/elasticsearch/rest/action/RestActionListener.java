/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.tasks.TaskCancelledException;

/**
 * An action listener that requires {@link #processResponse(Object)} to be implemented
 * and will automatically handle failures.
 */
public abstract class RestActionListener<Response> implements ActionListener<Response> {

    // we use static here so we won't have to pass the actual logger each time for a very rare case of logging
    // where the settings don't matter that much
    private static final Logger logger = LogManager.getLogger(RestResponseListener.class);

    protected final RestChannel channel;

    protected RestActionListener(RestChannel channel) {
        this.channel = channel;
    }

    @Override
    public final void onResponse(Response response) {
        try {
            ensureOpen();
            processResponse(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    protected abstract void processResponse(Response response) throws Exception;

    protected void ensureOpen() {
        if (channel.request().getHttpChannel().isOpen() == false) {
            throw new TaskCancelledException("response channel [" + channel.request().getHttpChannel() + "] closed");
        }
    }

    @Override
    public final void onFailure(Exception e) {
        try {
            channel.sendResponse(new RestResponse(channel, e));
        } catch (Exception inner) {
            inner.addSuppressed(e);
            logger.error("failed to send failure response", inner);
        }
    }
}

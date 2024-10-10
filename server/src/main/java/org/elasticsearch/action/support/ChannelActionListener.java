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
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

import static org.elasticsearch.core.Strings.format;

public final class ChannelActionListener<Response extends TransportResponse> implements ActionListener<Response> {

    private static final Logger logger = LogManager.getLogger(ChannelActionListener.class);

    private final TransportChannel channel;

    public ChannelActionListener(TransportChannel channel) {
        this.channel = channel;
    }

    @Override
    public void onResponse(Response response) {
        try {
            channel.sendResponse(response);
        } catch (RuntimeException e) {
            final String message = format("channel [%s] threw exceptions on sendResponse", channel);
            assert false : new AssertionError(message, e);
            logger.error(() -> message, e);
            throw e;
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            channel.sendResponse(e);
        } catch (RuntimeException sendException) {
            sendException.addSuppressed(e);
            final String message = format("channel [%s] threw exceptions on sendResponse", channel);
            assert false : new AssertionError(message, sendException);
            logger.error(() -> message, sendException);
            throw sendException;
        }
    }

    @Override
    public String toString() {
        return "ChannelActionListener{" + channel + "}";
    }
}

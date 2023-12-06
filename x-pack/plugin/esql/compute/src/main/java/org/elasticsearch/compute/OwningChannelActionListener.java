/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;

/**
 * Wraps a {@link ChannelActionListener} and takes ownership of responses passed to
 * {@link org.elasticsearch.action.ActionListener#onResponse(Object)}; the reference count will be decreased once sending is done.
 *
 * Deprecated: use {@link ChannelActionListener} instead and ensure responses sent to it are properly closed after.
 */
@Deprecated(forRemoval = true)
public final class OwningChannelActionListener<Response extends TransportResponse> implements ActionListener<Response> {
    private final ChannelActionListener<Response> listener;

    public OwningChannelActionListener(TransportChannel channel) {
        this.listener = new ChannelActionListener<>(channel);
    }

    @Override
    public void onResponse(Response response) {
        ActionListener.respondAndRelease(listener, response);
    }

    @Override
    public void onFailure(Exception e) {
        listener.onFailure(e);
    }

    @Override
    public String toString() {
        return "OwningChannelActionListener{" + listener + "}";
    }

}

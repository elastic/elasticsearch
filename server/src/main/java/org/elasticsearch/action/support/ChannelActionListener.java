/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

public final class ChannelActionListener<
    Response extends TransportResponse, Request extends TransportRequest> implements ActionListener<Response> {

    private final TransportChannel channel;
    private final Request request;
    private final String actionName;

    public ChannelActionListener(TransportChannel channel, String actionName, Request request) {
        this.channel = channel;
        this.request = request;
        this.actionName = actionName;
    }

    @Override
    public void onResponse(Response response) {
        try {
            channel.sendResponse(response);
        } catch (Exception e) {
            onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        TransportChannel.sendErrorResponse(channel, actionName, request, e);
    }

    @Override
    public String toString() {
        return "ChannelActionListener{" + channel + "}{" + request + "}{" + actionName + "}";
    }
}

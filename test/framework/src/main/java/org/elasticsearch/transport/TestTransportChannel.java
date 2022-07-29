/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;

public class TestTransportChannel implements TransportChannel {

    private final ActionListener<TransportResponse> listener;

    public TestTransportChannel(ActionListener<TransportResponse> listener) {
        this.listener = listener;
    }

    @Override
    public String getProfileName() {
        return "default";
    }

    @Override
    public void sendResponse(TransportResponse response) {
        listener.onResponse(response);
    }

    @Override
    public void sendResponse(Exception exception) {
        listener.onFailure(exception);
    }

    @Override
    public String getChannelType() {
        return "test";
    }
}

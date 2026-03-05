/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;

/**
 * A test-only {@link TransportService.DirectResponseChannel} that delegates to an {@link ActionListener}
 * instead of interacting with a real {@link TransportService}. This allows testing code paths that branch
 * on {@link TransportService#isDirectResponseChannel}.
 */
public class TestDirectResponseChannel extends TransportService.DirectResponseChannel {

    private final ActionListener<TransportResponse> listener;

    public TestDirectResponseChannel(ActionListener<TransportResponse> listener) {
        super(null, "test", 0, null);
        this.listener = listener;
    }

    @Override
    public void sendResponse(TransportResponse response) {
        listener.onResponse(response);
    }

    @Override
    public void sendResponse(Exception exception) {
        listener.onFailure(exception);
    }
}

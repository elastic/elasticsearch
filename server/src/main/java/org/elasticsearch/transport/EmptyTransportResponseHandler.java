/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamInput;

public class EmptyTransportResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {

    private final ActionListener<Void> listener;

    public EmptyTransportResponseHandler(ActionListener<Void> listener) {
        this.listener = listener;
    }

    @Override
    public final void handleResponse(TransportResponse.Empty response) {
        listener.onResponse(null);
    }

    @Override
    public final void handleException(TransportException exp) {
        listener.onFailure(exp);
    }

    @Override
    public final TransportResponse.Empty read(StreamInput in) {
        return TransportResponse.Empty.INSTANCE;
    }
}

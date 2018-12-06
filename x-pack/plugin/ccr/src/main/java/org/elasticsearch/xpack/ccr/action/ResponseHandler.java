/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

final class ResponseHandler {

    private final AtomicInteger counter;
    private final AtomicReferenceArray<Object> responses;
    private final ActionListener<AcknowledgedResponse> listener;

    ResponseHandler(int numRequests, ActionListener<AcknowledgedResponse> listener) {
        this.counter = new AtomicInteger(numRequests);
        this.responses = new AtomicReferenceArray<>(numRequests);
        this.listener = listener;
    }

    <T> ActionListener<T> getActionListener(final int requestId) {
        return new ActionListener<T>() {

            @Override
            public void onResponse(T response) {
                responses.set(requestId, response);
                finalizeResponse();
            }

            @Override
            public void onFailure(Exception e) {
                responses.set(requestId, e);
                finalizeResponse();
            }
        };
    }

    private void finalizeResponse() {
        Exception error = null;
        if (counter.decrementAndGet() == 0) {
            for (int j = 0; j < responses.length(); j++) {
                Object response = responses.get(j);
                if (response instanceof Exception) {
                    if (error == null) {
                        error = (Exception) response;
                    } else {
                        error.addSuppressed((Exception) response);
                    }
                }
            }

            if (error == null) {
                listener.onResponse(new AcknowledgedResponse(true));
            } else {
                listener.onFailure(error);
            }
        }
    }
}

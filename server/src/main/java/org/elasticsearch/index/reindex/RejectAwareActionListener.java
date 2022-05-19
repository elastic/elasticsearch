/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.CheckedConsumer;

import java.util.function.Consumer;

// public for testing
public interface RejectAwareActionListener<T> extends ActionListener<T> {
    void onRejection(Exception e);

    /**
     * Return a new listener that delegates failure/reject to errorDelegate but forwards response to responseHandler
     */
    static <X> RejectAwareActionListener<X> withResponseHandler(RejectAwareActionListener<?> errorDelegate, Consumer<X> responseHandler) {
        return new RejectAwareActionListener<>() {
            @Override
            public void onRejection(Exception e) {
                errorDelegate.onRejection(e);
            }

            @Override
            public void onResponse(X t) {
                responseHandler.accept(t);
            }

            @Override
            public void onFailure(Exception e) {
                errorDelegate.onFailure(e);
            }
        };
    }

    /**
     * Similar to {@link ActionListener#wrap(CheckedConsumer, Consumer)}, extended to have handler for onRejection.
     */
    static <Response> RejectAwareActionListener<Response> wrap(
        CheckedConsumer<Response, ? extends Exception> onResponse,
        Consumer<Exception> onFailure,
        Consumer<Exception> onRejection
    ) {
        return new RejectAwareActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    onResponse.accept(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }

            @Override
            public void onRejection(Exception e) {
                onRejection.accept(e);
            }
        };
    }

}

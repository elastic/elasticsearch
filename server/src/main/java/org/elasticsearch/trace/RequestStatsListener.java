/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.trace;

import org.elasticsearch.action.ActionListener;

public class RequestStatsListener<Response> implements ActionListener<Response> {
    private final ActionListener<Response> delegate;
    private final RequestStatsService.RequestKind kind;

    private RequestStatsListener(RequestStatsService.RequestKind kind, ActionListener<Response> delegate) {
        this.kind = kind;
        this.delegate = delegate;
    }

    public static <Response> RequestStatsListener<Response> wrap(RequestStatsService.RequestKind kind, ActionListener<Response> delegate) {
        return new RequestStatsListener<>(kind, delegate);
    }

    public static <Response> ActionListener<Response> wrapIfEnabled(
        RequestStatsService.RequestKind kind,
        ActionListener<Response> delegate
    ) {
        if (RequestStatsService.getInstance().isEnabled() == false) {
            return delegate;
        }
        return new RequestStatsListener<>(kind, delegate);
    }

    @Override
    public void onResponse(Response response) {
        try {
            delegate.onResponse(response);
        } finally {
            RequestStatsService.getInstance().recordSuccess(kind);
        }
    }

    @Override
    public void onFailure(Exception e) {
        try {
            delegate.onFailure(e);
        } finally {
            RequestStatsService.getInstance().recordFailure(kind);
        }
    }
}

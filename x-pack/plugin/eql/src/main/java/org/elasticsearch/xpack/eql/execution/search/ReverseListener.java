/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.execution.payload.ReversePayload;
import org.elasticsearch.xpack.eql.session.Payload;

public class ReverseListener implements ActionListener<Payload> {

    private final ActionListener<Payload> delegate;

    public ReverseListener(ActionListener<Payload> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onResponse(Payload response) {
        delegate.onResponse(new ReversePayload(response));
    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.eql.execution.payload.ReversePayload;
import org.elasticsearch.xpack.eql.session.Payload;

public class ReverseListener extends ActionListener.Delegating<Payload, Payload> {

    public ReverseListener(ActionListener<Payload> delegate) {
        super(delegate);
    }

    @Override
    public void onResponse(Payload response) {
        delegate.onResponse(new ReversePayload(response));
    }
}

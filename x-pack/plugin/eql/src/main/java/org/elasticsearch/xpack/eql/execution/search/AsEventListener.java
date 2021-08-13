/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.search;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.xpack.eql.execution.payload.EventPayload;
import org.elasticsearch.xpack.eql.session.Payload;

public class AsEventListener extends ActionListener.Delegating<SearchResponse, Payload> {

    public AsEventListener(ActionListener<Payload> listener) {
        super(listener);
    }

    @Override
    public void onResponse(SearchResponse response) {
        delegate.onResponse(new EventPayload(response));
    }
}

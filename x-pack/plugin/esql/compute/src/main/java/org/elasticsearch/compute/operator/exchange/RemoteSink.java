/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;

public interface RemoteSink {
    void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener);

    /**
     * An empty remote sink, always responding as if it has completed.
     */
    RemoteSink EMPTY = (allSourcesFinished, listener) -> listener.onResponse(new ExchangeResponse(null, true));
}

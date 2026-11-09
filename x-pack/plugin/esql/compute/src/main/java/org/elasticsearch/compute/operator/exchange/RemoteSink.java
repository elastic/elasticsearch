/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;

public interface RemoteSink {

    /**
     * Tells this sink how many pages the remote side has sent in total, once the remote compute has finished. An
     * implementation can then report itself finished as soon as it has received that many pages, without waiting for
     * the fetch that would otherwise bring back the finished flag. The default ignores the hint.
     */
    default void allPagesSent(long pages) {}

    void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener);

    default void close(ActionListener<Void> listener) {
        fetchPageAsync(true, listener.delegateFailure((l, r) -> {
            final Page page = r.takePage();
            if (page != null) {
                page.releaseBlocks();
            }
            l.onResponse(null);
        }));
    }
}

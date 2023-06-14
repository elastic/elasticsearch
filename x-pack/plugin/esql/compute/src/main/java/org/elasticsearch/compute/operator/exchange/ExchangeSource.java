/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.compute.data.Page;

/**
 * Source for exchanging data
 * @see ExchangeSourceOperator
 */
public interface ExchangeSource {
    /**
     * Remove the page from this source if any; otherwise, returns null
     */
    Page pollPage();

    /**
     * Called when the source has enough input pages
     */
    void finish();

    /**
     * Whether the associated sinks are finished and pages are processed.
     */
    boolean isFinished();

    /**
     * Returns the number of pages that are buffered in this exchange source
     */
    int bufferSize();

    /**
     * Allows callers to stop reading from the source when it's blocked
     */
    ListenableActionFuture<Void> waitForReading();
}

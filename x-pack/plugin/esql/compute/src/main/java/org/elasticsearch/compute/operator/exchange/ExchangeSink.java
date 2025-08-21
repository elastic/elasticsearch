/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

/**
 * Sink for exchanging data
 * @see ExchangeSinkOperator
 */
public interface ExchangeSink {
    /**
     * adds a new page to this sink
     */
    void addPage(Page page);

    /**
     * called once all pages have been added (see {@link #addPage(Page)}).
     */
    void finish();

    /**
     * Whether the sink has received all pages
     */
    boolean isFinished();

    /**
     * Adds a listener that will be notified when this exchange sink is finished.
     */
    void addCompletionListener(ActionListener<Void> listener);

    /**
     * Whether the sink is blocked on adding more pages
     */
    IsBlockedResult waitForWriting();
}

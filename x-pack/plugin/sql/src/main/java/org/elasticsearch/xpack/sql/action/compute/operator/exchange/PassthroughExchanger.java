/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

/**
 * Exchanger that just passes through the data to the {@link ExchangeSource},
 * but limits the number of in-flight pages.
 */
public class PassthroughExchanger implements Exchanger {

    private final ExchangeSource exchangeSource;
    private final ExchangeMemoryManager bufferMemoryManager;

    /**
     * Creates a new pass-through exchanger
     * @param exchangeSource the exchange source to pass the data to
     * @param bufferMaxPages the maximum number of pages that should be buffered by the exchange source
     */
    public PassthroughExchanger(ExchangeSource exchangeSource, int bufferMaxPages) {
        this.exchangeSource = exchangeSource;
        bufferMemoryManager = new ExchangeMemoryManager(bufferMaxPages);
    }

    public PassthroughExchanger(ExchangeSource exchangeSource, ExchangeMemoryManager bufferMemoryManager) {
        this.exchangeSource = exchangeSource;
        this.bufferMemoryManager = bufferMemoryManager;
    }

    @Override
    public void accept(Page page) {
        bufferMemoryManager.addPage();
        exchangeSource.addPage(page, bufferMemoryManager::releasePage);
    }

    @Override
    public void finish() {
        exchangeSource.finish();
    }

    @Override
    public ListenableActionFuture<Void> waitForWriting() {
        return bufferMemoryManager.getNotFullFuture();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.Page;

public class PassthroughExchanger implements Exchanger {

    private final ExchangeSource exchangeSource;
    private final ExchangeMemoryManager bufferMemoryManager;

    public PassthroughExchanger(ExchangeSource exchangeSource, int bufferMaxPages) {
        this.exchangeSource = exchangeSource;
        bufferMemoryManager = new ExchangeMemoryManager(bufferMaxPages);
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

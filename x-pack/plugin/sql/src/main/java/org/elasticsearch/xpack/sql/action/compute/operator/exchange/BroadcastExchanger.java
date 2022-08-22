/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.util.List;
import java.util.function.Consumer;

/**
 * Broadcasts pages to multiple exchange sources
 */
public class BroadcastExchanger implements Exchanger {
    private final List<Consumer<ExchangeSource.PageReference>> buffers;
    private final ExchangeMemoryManager memoryManager;

    public BroadcastExchanger(List<Consumer<ExchangeSource.PageReference>> buffers, ExchangeMemoryManager memoryManager) {
        this.buffers = buffers;
        this.memoryManager = memoryManager;
    }

    @Override
    public void accept(Page page) {
        memoryManager.addPage();

        ExchangeSource.PageReference pageReference = new ExchangeSource.PageReference(page, new RunOnce(memoryManager::releasePage));

        for (Consumer<ExchangeSource.PageReference> buffer : buffers) {
            buffer.accept(pageReference);
        }
    }

    @Override
    public ListenableActionFuture<Void> waitForWriting() {
        return memoryManager.getNotFullFuture();
    }
}

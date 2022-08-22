/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Exchanger implementation that randomly hands off the data to various exchange sources.
 */
public class RandomExchanger implements Exchanger {

    private final List<Consumer<ExchangeSource.PageReference>> buffers;
    private final ExchangeMemoryManager memoryManager;

    public RandomExchanger(List<Consumer<Page>> buffers) {
        this.buffers = buffers.stream().map(b -> (Consumer<ExchangeSource.PageReference>) pageReference -> {
            pageReference.onRelease();
            b.accept(pageReference.page());
        }).collect(Collectors.toList());
        this.memoryManager = new ExchangeMemoryManager(Integer.MAX_VALUE);
    }

    public RandomExchanger(List<Consumer<ExchangeSource.PageReference>> buffers, ExchangeMemoryManager memoryManager) {
        this.buffers = buffers;
        this.memoryManager = memoryManager;
    }

    @Override
    public void accept(Page page) {
        int randomIndex = Randomness.get().nextInt(buffers.size());
        ExchangeSource.PageReference pageReference = new ExchangeSource.PageReference(page, memoryManager::releasePage);
        memoryManager.addPage();
        buffers.get(randomIndex).accept(pageReference);
    }

    @Override
    public ListenableActionFuture<Void> waitForWriting() {
        // TODO: implement
        return Operator.NOT_BLOCKED;
    }
}

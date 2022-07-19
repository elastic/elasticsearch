/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

public class RandomExchanger implements Exchanger {

    private final List<Consumer<Page>> buffers;

    public RandomExchanger(List<Consumer<Page>> buffers) {
        this.buffers = buffers;
    }

    @Override
    public void accept(Page page) {
        int randomIndex = ThreadLocalRandom.current().nextInt(buffers.size());
        buffers.get(randomIndex).accept(page);
    }

    @Override
    public ListenableActionFuture<Void> waitForWriting() {
        // TODO: implement
        return Operator.NOT_BLOCKED;
    }
}

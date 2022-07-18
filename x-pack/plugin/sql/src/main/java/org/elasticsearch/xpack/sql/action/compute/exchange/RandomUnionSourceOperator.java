/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomUnionSourceOperator implements Operator {

    private final List<ExchangeSource> sources;

    public RandomUnionSourceOperator(List<ExchangeSource> sources) {
        this.sources = sources;
    }

    @Override
    public Page getOutput() {
        int randomIndex = ThreadLocalRandom.current().nextInt(sources.size());
        return sources.get(randomIndex).removePage();
    }

    @Override
    public boolean isFinished() {
        return sources.stream().allMatch(ExchangeSource::isFinished);
    }

    @Override
    public void finish() {
        sources.forEach(ExchangeSource::finish);
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.ann.Experimental;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.List;

/**
 * Source operator implementation that interleaves the data from different exchange sources in
 * random fashion.
 */
@Experimental
public class RandomUnionSourceOperator extends SourceOperator {

    private final List<ExchangeSource> sources;

    public RandomUnionSourceOperator(List<ExchangeSource> sources) {
        this.sources = sources;
    }

    @Override
    public Page getOutput() {
        int randomIndex = Randomness.get().nextInt(sources.size());
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
    public void close() {

    }
}

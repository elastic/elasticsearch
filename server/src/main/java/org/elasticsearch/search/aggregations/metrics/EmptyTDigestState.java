/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.tdigest.Centroid;
import org.elasticsearch.tdigest.TDigest;

import java.util.List;

public final class EmptyTDigestState extends TDigestState {
    public EmptyTDigestState() {
        super(1.0D);
    }

    @Override
    public TDigest recordAllData() {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void add(double x, int w) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void add(List<? extends TDigest> others) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void compress() {}

    @Override
    public void add(double x) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public void add(TDigest other) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    protected Centroid createCentroid(double mean, int id) {
        throw new UnsupportedOperationException("Immutable Empty TDigest");
    }

    @Override
    public boolean isRecording() {
        return false;
    }
}

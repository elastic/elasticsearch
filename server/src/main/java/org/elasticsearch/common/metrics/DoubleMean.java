/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.metrics;

public class DoubleMean {
    public static final DoubleMean ZERO = new DoubleMean(0, 0);
    private final double sum;
    private final long count;

    DoubleMean(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double mean() {
        return count == 0 ? 0 : sum / count;
    }
}

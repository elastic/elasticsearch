/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation.function;

public class ClampFunction extends AbstractLastFunction {
    private final Double max;
    private final Double min;

    public ClampFunction(Double max, Double min) {
        this.max = max;
        this.min = min;
    }

    @Override
    protected Double interGet() {
        if (min > max) {
            return Double.NaN;
        }
        return Math.max(min, Math.min(max, getPoint().getValue()));
    }
}

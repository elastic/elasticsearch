/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries.aggregation;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.core.TimeValue;

public class RoundingInterval {
    private final long startTime;
    private final long interval;
    private final Rounding.Prepared rounding;

    public RoundingInterval(long startTime, long interval) {
        this.startTime = startTime;
        this.interval = interval;
        if (this.startTime <= 0) {
            rounding = Rounding.builder(new TimeValue(this.interval)).build().prepareForUnknown();
        } else {
            rounding = null;
        }
    }

    public long nextRoundingValue(long utcMillis) {
        if (rounding != null) {
            return rounding.nextRoundingValue(utcMillis - 1);
        } else {
            long step = (utcMillis - 1 - startTime) / interval + 1;
            return startTime + interval * step;
        }
    }
}

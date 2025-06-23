/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.util.Map;

final class CostEntry {
    final double usd_per_hour;

    CostEntry(double usdPerHour) {
        this.usd_per_hour = usdPerHour;
    }

    public static CostEntry fromSource(Map<String, Object> source) {
        var val = source.get("usd_per_hour");

        if (val instanceof Number n) {
            // Some JSON values have no decimal places and are passed in as Integers.
            return new CostEntry(n.doubleValue());
        } else if (val == null) {
            return new CostEntry(CostCalculator.DEFAULT_COST_USD_PER_CORE_HOUR * HostMetadata.DEFAULT_PROFILING_NUM_CORES);
        }

        throw new IllegalArgumentException("[" + val + "] is an invalid value for [usd_per_hour]");
    }
}

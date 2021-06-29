/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.profile.AbstractProfiler;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AggregationProfiler extends AbstractProfiler<AggregationProfileBreakdown, Aggregator> {

    private final Map<List<String>, AggregationProfileBreakdown> profileBreakdownLookup = new HashMap<>();

    public AggregationProfiler() {
        super(new InternalAggregationProfileTree());
    }

    @Override
    public AggregationProfileBreakdown getQueryBreakdown(Aggregator agg) {
        List<String> path = getAggregatorPath(agg);
        AggregationProfileBreakdown aggregationProfileBreakdown = profileBreakdownLookup.get(path);
        if (aggregationProfileBreakdown == null) {
            aggregationProfileBreakdown = super.getQueryBreakdown(agg);
            profileBreakdownLookup.put(path, aggregationProfileBreakdown);
        }
        return aggregationProfileBreakdown;
    }

    public static List<String> getAggregatorPath(Aggregator agg) {
        LinkedList<String> path = new LinkedList<>();
        while (agg != null) {
            path.addFirst(agg.name());
            agg = agg.parent();
        }
        return path;
    }
}

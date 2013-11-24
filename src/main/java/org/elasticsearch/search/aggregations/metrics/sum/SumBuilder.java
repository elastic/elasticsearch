package org.elasticsearch.search.aggregations.metrics.sum;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class SumBuilder extends ValuesSourceMetricsAggregationBuilder<SumBuilder> {

    public SumBuilder(String name) {
        super(name, InternalSum.TYPE.name());
    }
}

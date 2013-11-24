package org.elasticsearch.search.aggregations.metrics.avg;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class AvgBuilder extends ValuesSourceMetricsAggregationBuilder<AvgBuilder> {

    public AvgBuilder(String name) {
        super(name, InternalAvg.TYPE.name());
    }
}

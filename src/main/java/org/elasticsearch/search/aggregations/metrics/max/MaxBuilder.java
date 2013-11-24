package org.elasticsearch.search.aggregations.metrics.max;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class MaxBuilder extends ValuesSourceMetricsAggregationBuilder<MaxBuilder> {

    public MaxBuilder(String name) {
        super(name, InternalMax.TYPE.name());
    }
}

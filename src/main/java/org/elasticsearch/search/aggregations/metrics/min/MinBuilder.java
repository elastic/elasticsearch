package org.elasticsearch.search.aggregations.metrics.min;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class MinBuilder extends ValuesSourceMetricsAggregationBuilder<MinBuilder> {

    public MinBuilder(String name) {
        super(name, InternalMin.TYPE.name());
    }
}

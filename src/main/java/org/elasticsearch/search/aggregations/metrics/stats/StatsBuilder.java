package org.elasticsearch.search.aggregations.metrics.stats;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class StatsBuilder extends ValuesSourceMetricsAggregationBuilder<StatsBuilder> {

    public StatsBuilder(String name) {
        super(name, InternalStats.TYPE.name());
    }
}

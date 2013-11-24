package org.elasticsearch.search.aggregations.metrics.stats.extended;

import org.elasticsearch.search.aggregations.metrics.ValuesSourceMetricsAggregationBuilder;

/**
 *
 */
public class ExtendedStatsBuilder extends ValuesSourceMetricsAggregationBuilder<ExtendedStatsBuilder> {

    public ExtendedStatsBuilder(String name) {
        super(name, InternalExtendedStats.TYPE.name());
    }
}

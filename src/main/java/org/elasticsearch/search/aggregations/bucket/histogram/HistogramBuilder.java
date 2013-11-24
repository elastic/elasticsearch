package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 *
 */
public class HistogramBuilder extends ValuesSourceAggregationBuilder<HistogramBuilder> {

    private Long interval;
    private HistogramBase.Order order;
    private Boolean computeEmptyBuckets;

    public HistogramBuilder(String name) {
        super(name, InternalHistogram.TYPE.name());
    }

    public HistogramBuilder interval(long interval) {
        this.interval = interval;
        return this;
    }

    public HistogramBuilder order(Histogram.Order order) {
        this.order = order;
        return this;
    }

    public HistogramBuilder emptyBuckets(boolean computeEmptyBuckets) {
        this.computeEmptyBuckets = computeEmptyBuckets;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (interval == null) {
            throw new SearchSourceBuilderException("[interval] must be defined for histogram aggregation [" + name + "]");
        }
        builder.field("interval", interval);

        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }

        if (computeEmptyBuckets != null) {
            builder.field("empty_buckets", computeEmptyBuckets);
        }

        return builder;
    }

}

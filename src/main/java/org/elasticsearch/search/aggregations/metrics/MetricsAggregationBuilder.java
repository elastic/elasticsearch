package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public abstract class MetricsAggregationBuilder<B extends MetricsAggregationBuilder<B>> extends AbstractAggregationBuilder {

    public MetricsAggregationBuilder(String name, String type) {
        super(name, type);
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name).startObject(type);
        internalXContent(builder, params);
        return builder.endObject().endObject();
    }

    protected abstract void internalXContent(XContentBuilder builder, Params params) throws IOException;
}

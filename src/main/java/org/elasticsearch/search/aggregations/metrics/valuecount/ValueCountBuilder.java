package org.elasticsearch.search.aggregations.metrics.valuecount;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class ValueCountBuilder extends MetricsAggregationBuilder<ValueCountBuilder> {

    private String field;

    public ValueCountBuilder(String name) {
        super(name, InternalValueCount.TYPE.name());
    }

    public ValueCountBuilder field(String field) {
        this.field = field;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (field != null) {
            builder.field("field", field);
        }
    }
}

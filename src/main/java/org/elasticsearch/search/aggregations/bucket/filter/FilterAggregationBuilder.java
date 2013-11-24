package org.elasticsearch.search.aggregations.bucket.filter;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 *
 */
public class FilterAggregationBuilder extends AggregationBuilder<FilterAggregationBuilder> {

    private FilterBuilder filter;

    public FilterAggregationBuilder(String name) {
        super(name, InternalFilter.TYPE.name());
    }

    public FilterAggregationBuilder filter(FilterBuilder filter) {
        this.filter = filter;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        if (filter == null) {
            throw new SearchSourceBuilderException("filter must be set on filter aggregation [" + name + "]");
        }
        filter.toXContent(builder, params);
        return builder;
    }
}

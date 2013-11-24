package org.elasticsearch.search.aggregations.bucket.nested;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 *
 */
public class NestedBuilder extends AggregationBuilder<NestedBuilder> {

    private String path;

    public NestedBuilder(String name) {
        super(name, InternalNested.TYPE.name());
    }

    public NestedBuilder path(String path) {
        this.path = path;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (path == null) {
            throw new SearchSourceBuilderException("nested path must be set on nested aggregation [" + name + "]");
        }
        builder.field("path", path);
        return builder.endObject();
    }
}

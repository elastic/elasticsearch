package org.elasticsearch.search.aggregations.bucket.global;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class GlobalBuilder extends AggregationBuilder<GlobalBuilder> {

    public GlobalBuilder(String name) {
        super(name, InternalGlobal.TYPE.name());
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }
}

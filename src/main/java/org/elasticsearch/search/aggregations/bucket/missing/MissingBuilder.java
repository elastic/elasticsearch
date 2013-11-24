package org.elasticsearch.search.aggregations.bucket.missing;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

/**
 *
 */
public class MissingBuilder extends AggregationBuilder<MissingBuilder> {

    private String field;

    public MissingBuilder(String name) {
        super(name, InternalMissing.TYPE.name());
    }

    public MissingBuilder field(String field) {
        this.field = field;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field("field", field);
        }
        return builder.endObject();
    }
}

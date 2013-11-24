package org.elasticsearch.search.aggregations.bucket.range;

import com.google.common.collect.Lists;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.List;

/**
 *
 */
public abstract class RangeBuilderBase<B extends RangeBuilderBase<B>> extends ValuesSourceAggregationBuilder<B> {

    protected static class Range implements ToXContent {

        private String key;
        private Object from;
        private Object to;

        public Range(String key, Object from, Object to) {
            this.key = key;
            this.from = from;
            this.to = to;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (key != null) {
                builder.field("key", key);
            }
            if (from != null) {
                builder.field("from", from);
            }
            if (to != null) {
                builder.field("to", to);
            }
            return builder.endObject();
        }
    }

    protected List<Range> ranges = Lists.newArrayList();

    protected RangeBuilderBase(String name, String type) {
        super(name, type);
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        if (ranges.isEmpty()) {
            throw new SearchSourceBuilderException("at least one range must be defined for range aggregation [" + name + "]");
        }
        builder.startArray("ranges");
        for (Range range : ranges) {
            range.toXContent(builder, params);
        }
        return builder.endArray();
    }
}

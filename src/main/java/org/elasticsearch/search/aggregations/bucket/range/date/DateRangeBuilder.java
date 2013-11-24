package org.elasticsearch.search.aggregations.bucket.range.date;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeBuilderBase;

import java.io.IOException;

/**
 *
 */
public class DateRangeBuilder extends RangeBuilderBase<DateRangeBuilder> {

    private String format;

    public DateRangeBuilder(String name) {
        super(name, InternalDateRange.TYPE.name());
    }

    public DateRangeBuilder addRange(String key, Object from, Object to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public DateRangeBuilder addRange(Object from, Object to) {
        return addRange(null, from, to);
    }

    public DateRangeBuilder addUnboundedTo(String key, Object to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public DateRangeBuilder addUnboundedTo(Object to) {
        return addUnboundedTo(null, to);
    }

    public DateRangeBuilder addUnboundedFrom(String key, Object from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public DateRangeBuilder addUnboundedFrom(Object from) {
        return addUnboundedFrom(null, from);
    }

    public DateRangeBuilder format(String format) {
        this.format = format;
        return this;
    }

    @Override
    protected XContentBuilder doInternalXContent(XContentBuilder builder, Params params) throws IOException {
        super.doInternalXContent(builder, params);
        builder.field("format", format);
        return builder;
    }
}

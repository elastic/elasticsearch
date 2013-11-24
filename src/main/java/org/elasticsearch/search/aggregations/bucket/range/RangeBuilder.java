package org.elasticsearch.search.aggregations.bucket.range;

/**
 *
 */
public class RangeBuilder extends RangeBuilderBase<RangeBuilder> {

    public RangeBuilder(String name) {
        super(name, InternalRange.TYPE.name());
    }


    public RangeBuilder addRange(String key, double from, double to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public RangeBuilder addRange(double from, double to) {
        return addRange(null, from, to);
    }

    public RangeBuilder addUnboundedTo(String key, double to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public RangeBuilder addUnboundedTo(double to) {
        return addUnboundedTo(null, to);
    }

    public RangeBuilder addUnboundedFrom(String key, double from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public RangeBuilder addUnboundedFrom(double from) {
        return addUnboundedFrom(null, from);
    }

}

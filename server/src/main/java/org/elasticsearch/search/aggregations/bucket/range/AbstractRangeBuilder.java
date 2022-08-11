/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public abstract class AbstractRangeBuilder<AB extends AbstractRangeBuilder<AB, R>, R extends Range> extends ValuesSourceAggregationBuilder<
    AB> {

    protected final InternalRange.Factory<?, ?> rangeFactory;
    protected List<R> ranges = new ArrayList<>();
    protected boolean keyed = false;

    protected AbstractRangeBuilder(String name, InternalRange.Factory<?, ?> rangeFactory) {
        super(name);
        this.rangeFactory = rangeFactory;
    }

    protected AbstractRangeBuilder(
        AbstractRangeBuilder<AB, R> clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.rangeFactory = clone.rangeFactory;
        this.ranges = new ArrayList<>(clone.ranges);
        this.keyed = clone.keyed;
    }

    /**
     * Read from a stream.
     */
    protected AbstractRangeBuilder(StreamInput in, InternalRange.Factory<?, ?> rangeFactory, Writeable.Reader<R> rangeReader)
        throws IOException {
        super(in);
        this.rangeFactory = rangeFactory;
        ranges = in.readList(rangeReader);
        keyed = in.readBoolean();
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        // Copied over from the old targetValueType setting. Not sure what cases this is still relevant for. --Tozzi 2020-01-13
        return rangeFactory.getValueSourceType();
    }

    /**
     * Resolve any strings in the ranges so we have a number value for the from
     * and to of each range. The ranges are also sorted before being returned.
     */
    protected Range[] processRanges(Function<Range, Range> rangeProcessor) {
        Range[] ranges = new Range[this.ranges.size()];
        for (int i = 0; i < ranges.length; i++) {
            ranges[i] = rangeProcessor.apply(this.ranges.get(i));
        }
        sortRanges(ranges);
        return ranges;
    }

    /**
     * Sort the provided ranges in place.
     */
    static void sortRanges(final Range[] ranges) {
        new InPlaceMergeSorter() {

            @Override
            protected void swap(int i, int j) {
                final Range tmp = ranges[i];
                ranges[i] = ranges[j];
                ranges[j] = tmp;
            }

            @Override
            protected int compare(int i, int j) {
                int cmp = Double.compare(ranges[i].from, ranges[j].from);
                if (cmp == 0) {
                    cmp = Double.compare(ranges[i].to, ranges[j].to);
                }
                return cmp;
            }
        }.sort(0, ranges.length);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(ranges.size());
        for (Range range : ranges) {
            range.writeTo(out);
        }
        out.writeBoolean(keyed);
    }

    @SuppressWarnings("unchecked")
    public AB addRange(R range) {
        if (range == null) {
            throw new IllegalArgumentException("[range] must not be null: [" + name + "]");
        }
        ranges.add(range);
        return (AB) this;
    }

    public List<R> ranges() {
        return ranges;
    }

    @SuppressWarnings("unchecked")
    public AB keyed(boolean keyed) {
        this.keyed = keyed;
        return (AB) this;
    }

    public boolean keyed() {
        return keyed;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(RangeAggregator.RANGES_FIELD.getPreferredName(), ranges);
        builder.field(RangeAggregator.KEYED_FIELD.getPreferredName(), keyed);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), ranges, keyed);
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        AbstractRangeBuilder<AB, R> other = (AbstractRangeBuilder<AB, R>) obj;
        return Objects.equals(ranges, other.ranges) && Objects.equals(keyed, other.keyed);
    }
}

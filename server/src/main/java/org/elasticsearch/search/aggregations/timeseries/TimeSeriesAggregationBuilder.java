/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.timeseries;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.xcontent.InstantiatingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ParserConstructor;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TimeSeriesAggregationBuilder extends AbstractAggregationBuilder<TimeSeriesAggregationBuilder> {
    public static final String NAME = "time_series";
    public static final ParseField KEYED_FIELD = new ParseField("keyed");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final InstantiatingObjectParser<TimeSeriesAggregationBuilder, String> PARSER;

    private boolean keyed;
    private int size;

    static {
        InstantiatingObjectParser.Builder<TimeSeriesAggregationBuilder, String> parser = InstantiatingObjectParser.builder(
            NAME,
            false,
            TimeSeriesAggregationBuilder.class
        );
        parser.declareBoolean(optionalConstructorArg(), KEYED_FIELD);
        parser.declareInt(optionalConstructorArg(), SIZE_FIELD);
        PARSER = parser.build();
    }

    public TimeSeriesAggregationBuilder(String name) {
        this(name, true, Integer.MAX_VALUE);
    }

    @ParserConstructor
    public TimeSeriesAggregationBuilder(String name, Boolean keyed, Integer size) {
        super(name);
        this.keyed = keyed != null ? keyed : true;
        this.size = size != null ? size : Integer.MAX_VALUE;
        if (this.size <= 0) {
            throw new IllegalArgumentException("Expect size > 0, got [" + size + "]");
        }
    }

    protected TimeSeriesAggregationBuilder(
        TimeSeriesAggregationBuilder clone,
        AggregatorFactories.Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.keyed = clone.keyed;
        this.size = clone.size;
    }

    public TimeSeriesAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        keyed = in.readBoolean();
        size = in.readInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeBoolean(keyed);
        out.writeInt(size);
    }

    @Override
    protected AggregatorFactory doBuild(
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        return new TimeSeriesAggregationFactory(name, keyed, context, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(KEYED_FIELD.getPreferredName(), keyed);
        builder.field(SIZE_FIELD.getPreferredName(), size);
        builder.endObject();
        return builder;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TimeSeriesAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public int numberOfDocumentsInSortOrderExecution() {
        return size;
    }

    public boolean isKeyed() {
        return keyed;
    }

    public void setKeyed(boolean keyed) {
        this.keyed = keyed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        TimeSeriesAggregationBuilder that = (TimeSeriesAggregationBuilder) o;
        return keyed == that.keyed && size == that.size;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyed, size);
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_8_1_0;
    }
}

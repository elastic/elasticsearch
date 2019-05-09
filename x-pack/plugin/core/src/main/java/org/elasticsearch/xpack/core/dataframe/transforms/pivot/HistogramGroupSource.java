/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.dataframe.transforms.pivot;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class HistogramGroupSource extends SingleGroupSource {

    static final ParseField INTERVAL = new ParseField("interval");
    private static final String NAME = "data_frame_histogram_group";
    private static final ConstructingObjectParser<HistogramGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<HistogramGroupSource, Void> LENIENT_PARSER = createParser(true);
    private final double interval;

    public HistogramGroupSource(String field, double interval) {
        super(field);
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0.");
        }
        this.interval = interval;
    }

    public HistogramGroupSource(StreamInput in) throws IOException {
        super(in);
        interval = in.readDouble();
    }

    private static ConstructingObjectParser<HistogramGroupSource, Void> createParser(boolean lenient) {
        ConstructingObjectParser<HistogramGroupSource, Void> parser = new ConstructingObjectParser<>(NAME, lenient, (args) -> {
            String field = (String) args[0];
            double interval = (double) args[1];
            return new HistogramGroupSource(field, interval);
        });
        declareValuesSourceFields(parser);
        parser.declareDouble(optionalConstructorArg(), INTERVAL);
        return parser;
    }

    @Override
    public Type getType() {
        return Type.HISTOGRAM;
    }

    public static HistogramGroupSource fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(field);
        out.writeDouble(interval);
    }

    public double getInterval() {
        return interval;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (field != null) {
            builder.field(FIELD.getPreferredName(), field);
        }
        builder.field(INTERVAL.getPreferredName(), interval);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final HistogramGroupSource that = (HistogramGroupSource) other;

        return Objects.equals(this.field, that.field) &&
            Objects.equals(this.interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, interval);
    }

    @Override
    public QueryBuilder getIncrementalBucketUpdateFilterQuery(Set<String> changedBuckets) {
        // histograms are simple and cheap, so we skip this optimization
        return null;
    }

    @Override
    public boolean supportsIncrementalBucketUpdate() {
        return false;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class HistogramGroupSource extends SingleGroupSource {

    static final ParseField INTERVAL = new ParseField("interval");
    private static final String NAME = "data_frame_histogram_group";
    private static final ConstructingObjectParser<HistogramGroupSource, Void> STRICT_PARSER = createParser(false);
    private static final ConstructingObjectParser<HistogramGroupSource, Void> LENIENT_PARSER = createParser(true);
    private final double interval;

    public HistogramGroupSource(String field, ScriptConfig scriptConfig, boolean missingBucket, double interval) {
        super(field, scriptConfig, missingBucket);
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
            ScriptConfig scriptConfig = (ScriptConfig) args[1];
            boolean missingBucket = args[2] == null ? false : (boolean) args[2];
            double intervalValue = (double) args[3];
            return new HistogramGroupSource(field, scriptConfig, missingBucket, intervalValue);
        });
        declareValuesSourceFields(parser, lenient);
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
        super.writeTo(out);
        out.writeDouble(interval);
    }

    public double getInterval() {
        return interval;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        super.innerXContent(builder, params);
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

        return this.missingBucket == that.missingBucket
            && Objects.equals(this.field, that.field)
            && Objects.equals(this.scriptConfig, that.scriptConfig)
            && Objects.equals(this.interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, scriptConfig, interval);
    }
}

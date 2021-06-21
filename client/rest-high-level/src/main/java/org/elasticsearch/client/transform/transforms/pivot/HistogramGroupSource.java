/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A grouping via a histogram aggregation referencing a numeric field
 */
public class HistogramGroupSource extends SingleGroupSource implements ToXContentObject {

    protected static final ParseField INTERVAL = new ParseField("interval");
    private static final ConstructingObjectParser<HistogramGroupSource, Void> PARSER = new ConstructingObjectParser<>(
        "histogram_group_source",
        true,
        args -> new HistogramGroupSource((String) args[0], (Script) args[1], args[2] == null ? false : (boolean) args[2], (double) args[3])
    );

    static {
        PARSER.declareString(optionalConstructorArg(), FIELD);
        Script.declareScript(PARSER, optionalConstructorArg(), SCRIPT);
        PARSER.declareBoolean(optionalConstructorArg(), MISSING_BUCKET);
        PARSER.declareDouble(optionalConstructorArg(), INTERVAL);
    }

    public static HistogramGroupSource fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final double interval;

    HistogramGroupSource(String field, Script script, double interval) {
        this(field, script, false, interval);
    }

    HistogramGroupSource(String field, Script script, boolean missingBucket, double interval) {
        super(field, script, missingBucket);
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be greater than 0.");
        }
        this.interval = interval;
    }

    @Override
    public Type getType() {
        return Type.HISTOGRAM;
    }

    public double getInterval() {
        return interval;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
            && Objects.equals(this.script, that.script)
            && Objects.equals(this.interval, that.interval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, script, interval, missingBucket);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String field;
        private Script script;
        private boolean missingBucket;
        private double interval;

        /**
         * The field to reference in the histogram grouping
         * @param field The numeric field name to use in the histogram grouping
         * @return The {@link Builder} with the field set.
         */
        public Builder setField(String field) {
            this.field = field;
            return this;
        }

        /**
         * Set the interval for the histogram grouping
         * @param interval The numeric interval for the histogram grouping
         * @return The {@link Builder} with the interval set.
         */
        public Builder setInterval(double interval) {
            this.interval = interval;
            return this;
        }

        /**
         * The script with which to construct the histogram grouping
         * @param script The script
         * @return The {@link Builder} with the script set.
         */
        public Builder setScript(Script script) {
            this.script = script;
            return this;
        }

        /**
         * Sets the value of "missing_bucket"
         * @param missingBucket value of "missing_bucket" to be set
         * @return The {@link Builder} with "missing_bucket" set.
         */
        public Builder setMissingBucket(boolean missingBucket) {
            this.missingBucket = missingBucket;
            return this;
        }

        public HistogramGroupSource build() {
            return new HistogramGroupSource(field, script, missingBucket, interval);
        }
    }
}

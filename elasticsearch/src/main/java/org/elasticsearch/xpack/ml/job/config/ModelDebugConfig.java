/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.ml.job.messages.Messages;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class ModelDebugConfig extends ToXContentToBytes implements Writeable {

    private static final double MAX_PERCENTILE = 100.0;

    private static final ParseField TYPE_FIELD = new ParseField("model_debug_config");
    public static final ParseField BOUNDS_PERCENTILE_FIELD = new ParseField("bounds_percentile");
    public static final ParseField TERMS_FIELD = new ParseField("terms");

    public static final ConstructingObjectParser<ModelDebugConfig, Void> PARSER = new ConstructingObjectParser<>(
            TYPE_FIELD.getPreferredName(), a -> new ModelDebugConfig((Double) a[0], (String) a[1]));

    static {
        PARSER.declareDouble(ConstructingObjectParser.constructorArg(), BOUNDS_PERCENTILE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TERMS_FIELD);
    }

    private final double boundsPercentile;
    private final String terms;

    public ModelDebugConfig(double boundsPercentile, String terms) {
        if (boundsPercentile < 0.0 || boundsPercentile > MAX_PERCENTILE) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_MODEL_DEBUG_CONFIG_INVALID_BOUNDS_PERCENTILE);
            throw new IllegalArgumentException(msg);
        }
        this.boundsPercentile = boundsPercentile;
        this.terms = terms;
    }

    public ModelDebugConfig(StreamInput in) throws IOException {
        boundsPercentile = in.readDouble();
        terms = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(boundsPercentile);
        out.writeOptionalString(terms);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(BOUNDS_PERCENTILE_FIELD.getPreferredName(), boundsPercentile);
        if (terms != null) {
            builder.field(TERMS_FIELD.getPreferredName(), terms);
        }
        builder.endObject();
        return builder;
    }

    public double getBoundsPercentile() {
        return this.boundsPercentile;
    }

    public String getTerms() {
        return this.terms;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof ModelDebugConfig == false) {
            return false;
        }

        ModelDebugConfig that = (ModelDebugConfig) other;
        return Objects.equals(this.boundsPercentile, that.boundsPercentile) && Objects.equals(this.terms, that.terms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(boundsPercentile, terms);
    }
}

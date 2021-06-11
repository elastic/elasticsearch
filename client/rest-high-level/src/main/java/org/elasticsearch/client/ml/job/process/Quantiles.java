/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.job.process;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Date;
import java.util.Objects;

/**
 * Quantiles Result POJO
 */
public class Quantiles implements ToXContentObject {

    /**
     * Field Names
     */
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField QUANTILE_STATE = new ParseField("quantile_state");

    public static final ConstructingObjectParser<Quantiles, Void> PARSER =
        new ConstructingObjectParser<>("quantiles", true, a -> new Quantiles((String) a[0], (Date) a[1], (String) a[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(), p -> new Date(p.longValue()), TIMESTAMP, ValueType.LONG);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), QUANTILE_STATE);
    }

    private final String jobId;
    private final Date timestamp;
    private final String quantileState;

    public Quantiles(String jobId, Date timestamp, String quantileState) {
        this.jobId = jobId;
        this.timestamp = Objects.requireNonNull(timestamp);
        this.quantileState = Objects.requireNonNull(quantileState);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        if (timestamp != null) {
            builder.field(TIMESTAMP.getPreferredName(), timestamp.getTime());
        }
        if (quantileState != null) {
            builder.field(QUANTILE_STATE.getPreferredName(), quantileState);
        }
        builder.endObject();
        return builder;
    }

    public String getJobId() {
        return jobId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getQuantileState() {
        return quantileState;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, timestamp, quantileState);
    }

    /**
     * Compare all the fields.
     */
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        Quantiles that = (Quantiles) other;

        return Objects.equals(this.jobId, that.jobId) && Objects.equals(this.timestamp, that.timestamp)
            && Objects.equals(this.quantileState, that.quantileState);
    }
}


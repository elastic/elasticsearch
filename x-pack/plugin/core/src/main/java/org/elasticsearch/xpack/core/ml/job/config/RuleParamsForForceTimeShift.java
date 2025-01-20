/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class RuleParamsForForceTimeShift implements ToXContentObject, Writeable {
    public static final ParseField TYPE_FIELD = new ParseField("force_time_shift_params");
    public static final ParseField TIME_SHIFT_AMOUNT_FIELD = new ParseField("time_shift_amount");

    public static final ConstructingObjectParser<RuleParamsForForceTimeShift, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<RuleParamsForForceTimeShift, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<RuleParamsForForceTimeShift, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RuleParamsForForceTimeShift, Void> parser = new ConstructingObjectParser<>(
            TYPE_FIELD.getPreferredName(),
            ignoreUnknownFields,
            a -> new RuleParamsForForceTimeShift((Long) a[0])
        );
        parser.declareLong(ConstructingObjectParser.constructorArg(), TIME_SHIFT_AMOUNT_FIELD);
        return parser;
    }

    private final long timeShiftAmount;

    public RuleParamsForForceTimeShift(long timeShiftAmount) {
        this.timeShiftAmount = timeShiftAmount;
    }

    public RuleParamsForForceTimeShift(StreamInput in) throws IOException {
        timeShiftAmount = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timeShiftAmount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIME_SHIFT_AMOUNT_FIELD.getPreferredName(), timeShiftAmount);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RuleParamsForForceTimeShift == false) {
            return false;
        }

        RuleParamsForForceTimeShift other = (RuleParamsForForceTimeShift) obj;
        return timeShiftAmount == other.timeShiftAmount;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(timeShiftAmount);
    }
}

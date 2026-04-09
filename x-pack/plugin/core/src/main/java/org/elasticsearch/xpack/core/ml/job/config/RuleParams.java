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
import java.util.Objects;

public class RuleParams implements ToXContentObject, Writeable {

    public static final ParseField RULE_PARAMS_FIELD = new ParseField("params");
    public static final ParseField FORCE_TIME_SHIFT_FIELD = new ParseField("force_time_shift");

    // These parsers follow the pattern that metadata is parsed leniently (to allow for enhancements), whilst config is parsed strictly
    public static final ConstructingObjectParser<RuleParams, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<RuleParams, Void> STRICT_PARSER = createParser(false);

    public static ConstructingObjectParser<RuleParams, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<RuleParams, Void> parser = new ConstructingObjectParser<>(
            RULE_PARAMS_FIELD.getPreferredName(),
            ignoreUnknownFields,
            a -> new RuleParams((RuleParamsForForceTimeShift) a[0])
        );

        parser.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            RuleParamsForForceTimeShift.LENIENT_PARSER,
            FORCE_TIME_SHIFT_FIELD
        );
        return parser;
    }

    private final RuleParamsForForceTimeShift forceTimeShift;

    public RuleParams() {
        this.forceTimeShift = null;
    }

    public RuleParams(RuleParamsForForceTimeShift forceTimeShift) {
        this.forceTimeShift = forceTimeShift;
    }

    public RuleParams(StreamInput in) throws IOException {
        // initialize optional forceTimeShift from in
        forceTimeShift = in.readOptionalWriteable(RuleParamsForForceTimeShift::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // write optional forceTimeShift to out
        out.writeOptionalWriteable(forceTimeShift);
    }

    boolean isEmpty() {
        return forceTimeShift == null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (forceTimeShift != null) {
            builder.field(FORCE_TIME_SHIFT_FIELD.getPreferredName(), forceTimeShift);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj instanceof RuleParams == false) {
            return false;
        }

        RuleParams other = (RuleParams) obj;
        return Objects.equals(forceTimeShift, other.forceTimeShift);
    }

    @Override
    public int hashCode() {
        return Objects.hash(forceTimeShift);
    }

    public RuleParamsForForceTimeShift getForceTimeShift() {
        return forceTimeShift;
    }
}

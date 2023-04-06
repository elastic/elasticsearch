/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for index minimum age. Evaluates to <code>true</code>
 * when the index is at least {@link #value} old
 */
public class MinAgeCondition extends Condition<TimeValue> {
    public static final String NAME = "min_age";

    public MinAgeCondition(TimeValue value) {
        super(NAME, Type.MIN);
        this.value = value;
    }

    public MinAgeCondition(StreamInput in) throws IOException {
        super(NAME, Type.MIN);
        this.value = in.readTimeValue();
    }

    @Override
    public Result evaluate(final Stats stats) {
        long indexAge = System.currentTimeMillis() - stats.indexCreated();
        return new Result(this, this.value.getMillis() <= indexAge);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MinAgeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MinAgeCondition(TimeValue.parseTimeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(TransportVersion version) {
        return version.onOrAfter(TransportVersion.V_8_4_0);
    }
}

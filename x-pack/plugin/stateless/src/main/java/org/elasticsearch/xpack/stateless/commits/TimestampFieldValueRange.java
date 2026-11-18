/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The range of the values of the {@code @timestamp} field. It is not part of {@link StatelessCompoundCommit} because it is used by the
 * header of compound commits and by the headers of other blobs, and by the code that timestamps the cache regions and computes the range
 * from the segments of the engine. Its XContent encoding is persisted in those headers.
 */
public record TimestampFieldValueRange(long minMillis, long maxMillis) implements Writeable, ToXContentObject {

    static final ConstructingObjectParser<TimestampFieldValueRange, Void> TIMESTAMP_FIELD_VALUE_RANGE_PARSER =
        new ConstructingObjectParser<>("timestamp_field_value_range", args -> new TimestampFieldValueRange((long) args[0], (long) args[1]));

    static {
        TIMESTAMP_FIELD_VALUE_RANGE_PARSER.declareLong(constructorArg(), new ParseField("min_millis"));
        TIMESTAMP_FIELD_VALUE_RANGE_PARSER.declareLong(constructorArg(), new ParseField("max_millis"));
    }

    public TimestampFieldValueRange {
        if (minMillis > maxMillis) {
            throw new IllegalArgumentException("Invalid millis timestamp range [" + minMillis + ", " + maxMillis + "]");
        }
    }

    public TimestampFieldValueRange(StreamInput in) throws IOException {
        this(in.readLong(), in.readLong());
    }

    public long midpointMillis() {
        return minMillis + (maxMillis - minMillis) / 2;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(minMillis);
        out.writeLong(maxMillis);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("min_millis", minMillis).field("max_millis", maxMillis).endObject();
    }
}

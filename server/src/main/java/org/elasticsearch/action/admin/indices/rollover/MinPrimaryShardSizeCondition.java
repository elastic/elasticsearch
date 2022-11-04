/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * A size-based condition for the primary shards within an index.
 * Evaluates to <code>true</code> if the size of the largest primary shard is at least {@link #value}.
 */
public class MinPrimaryShardSizeCondition extends Condition<ByteSizeValue> {
    public static final String NAME = "min_primary_shard_size";

    public MinPrimaryShardSizeCondition(ByteSizeValue value) {
        super(NAME, Type.MIN);
        this.value = value;
    }

    public MinPrimaryShardSizeCondition(StreamInput in) throws IOException {
        super(NAME, Type.MIN);
        this.value = ByteSizeValue.readFrom(in);
    }

    @Override
    public Result evaluate(Stats stats) {
        return new Result(this, stats.maxPrimaryShardSize().getBytes() >= value.getBytes());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        value.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value.getStringRep());
    }

    public static MinPrimaryShardSizeCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new MinPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(parser.text(), NAME));
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(Version version) {
        return version.onOrAfter(Version.V_8_4_0);
    }
}

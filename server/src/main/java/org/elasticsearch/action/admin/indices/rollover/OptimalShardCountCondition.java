/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for automatically increasing the number of shards for a data stream. This indicates the optimum number of shards that was
 * configured for the index abstraction as part of rollover.
 * It's more of a marker condition, when present the condition is met, more than a condition we evaluate against stats.
 */
public class OptimalShardCountCondition extends Condition<Integer> {
    public static final String NAME = "optimal_shard_count";

    public OptimalShardCountCondition(int optimalShards) {
        super(NAME, Type.AUTOMATIC);
        this.value = optimalShards;
    }

    public OptimalShardCountCondition(StreamInput in) throws IOException {
        super(NAME, Type.AUTOMATIC);
        this.value = in.readVInt();
    }

    @Override
    public Result evaluate(final Stats stats) {
        return new Result(this, true);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value);
    }

    public static OptimalShardCountCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_NUMBER) {
            return new OptimalShardCountCondition(parser.intValue());
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(TransportVersion version) {
        return version.onOrAfter(TransportVersions.V_8_14_0);
    }
}

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
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Condition for maximum shard docs. Evaluates to <code>true</code>
 * when a primary shard in the index has at least {@link #value} docs
 */
public class MaxPrimaryShardDocsCondition extends Condition<Long> {
    public static final String NAME = "max_primary_shard_docs";

    public MaxPrimaryShardDocsCondition(Long value) {
        super(NAME, Type.MAX);
        this.value = value;
    }

    public MaxPrimaryShardDocsCondition(StreamInput in) throws IOException {
        super(NAME, Type.MAX);
        this.value = in.readLong();
    }

    @Override
    public Result evaluate(Stats stats) {
        return new Result(this, this.value <= stats.maxPrimaryShardDocs());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value);
    }

    public static MaxPrimaryShardDocsCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_NUMBER) {
            return new MaxPrimaryShardDocsCondition(parser.longValue());
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(Version version) {
        return version.onOrAfter(Version.V_8_2_0);
    }
}

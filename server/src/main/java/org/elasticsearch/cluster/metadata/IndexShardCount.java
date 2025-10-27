/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Used to store the shard count for an index
 * Prior to v9.3, the entire {@link IndexMetadata} object was stored in heap and then loaded during snapshotting to determine
 * the shard count. As per ES-12539, this was replaced with {@link IndexShardCount} that tracks writes and loads only the index's
 * shard count to and from heap memory. This not only reduces the likelihood of a node going OOMe, but increases snapshotting performance.
 */
public class IndexShardCount implements ToXContentFragment {
    private static final String KEY_SHARD_COUNT = "shard_count";
    private final int shardCount;

    public IndexShardCount(int count) {
        this.shardCount = count;
    }

    public int getCount() {
        return shardCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_SHARD_COUNT, shardCount);
        return builder;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int count;

        public Builder setCount(int count) {
            this.count = count;
            return this;
        }

        public IndexShardCount build() {
            return new IndexShardCount(count);
        }
    }

    public static IndexShardCount fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        XContentParser.Token currentToken = parser.nextToken();
        IndexShardCount x;
        if (currentToken.isValue()) {
            x = new IndexShardCount(parser.intValue());
        } else {
            throw new IllegalArgumentException("Unexpected token " + currentToken);
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return x;
    }
}

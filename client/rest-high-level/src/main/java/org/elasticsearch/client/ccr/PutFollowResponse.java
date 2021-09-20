/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public final class PutFollowResponse {

    static final ParseField FOLLOW_INDEX_CREATED = new ParseField("follow_index_created");
    static final ParseField FOLLOW_INDEX_SHARDS_ACKED = new ParseField("follow_index_shards_acked");
    static final ParseField INDEX_FOLLOWING_STARTED = new ParseField("index_following_started");

    private static final ConstructingObjectParser<PutFollowResponse, Void> PARSER = new ConstructingObjectParser<>(
        "put_follow_response", true, args -> new PutFollowResponse((boolean) args[0], (boolean) args[1], (boolean) args[2]));

    static {
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), FOLLOW_INDEX_CREATED);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), FOLLOW_INDEX_SHARDS_ACKED);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), INDEX_FOLLOWING_STARTED);
    }

    public static PutFollowResponse fromXContent(final XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final boolean followIndexCreated;
    private final boolean followIndexShardsAcked;
    private final boolean indexFollowingStarted;

    public PutFollowResponse(boolean followIndexCreated, boolean followIndexShardsAcked, boolean indexFollowingStarted) {
        this.followIndexCreated = followIndexCreated;
        this.followIndexShardsAcked = followIndexShardsAcked;
        this.indexFollowingStarted = indexFollowingStarted;
    }

    public boolean isFollowIndexCreated() {
        return followIndexCreated;
    }

    public boolean isFollowIndexShardsAcked() {
        return followIndexShardsAcked;
    }

    public boolean isIndexFollowingStarted() {
        return indexFollowingStarted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutFollowResponse that = (PutFollowResponse) o;
        return followIndexCreated == that.followIndexCreated &&
            followIndexShardsAcked == that.followIndexShardsAcked &&
            indexFollowingStarted == that.indexFollowingStarted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(followIndexCreated, followIndexShardsAcked, indexFollowingStarted);
    }
}

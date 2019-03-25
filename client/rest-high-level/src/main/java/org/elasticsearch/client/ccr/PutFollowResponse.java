/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.ParseField;
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

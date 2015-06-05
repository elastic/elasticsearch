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

package org.elasticsearch.action.suggest;

import org.elasticsearch.action.support.broadcast.BroadcastShardResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.suggest.Suggest;

import java.io.IOException;

/**
 * Internal suggest response of a shard suggest request executed directly against a specific shard.
 */
class ShardSuggestResponse extends BroadcastShardResponse {

    private final Suggest suggest;

    ShardSuggestResponse() {
        this.suggest = new Suggest();
    }

    ShardSuggestResponse(ShardId shardId, Suggest suggest) {
        super(shardId);
        this.suggest = suggest;
    }

    public Suggest getSuggest() {
        return this.suggest;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        suggest.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        suggest.writeTo(out);
    }
}

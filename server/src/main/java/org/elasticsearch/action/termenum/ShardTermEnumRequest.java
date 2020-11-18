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

package org.elasticsearch.action.termenum;

import org.elasticsearch.action.support.broadcast.BroadcastShardRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.internal.AliasFilter;

import java.io.IOException;
import java.util.Objects;

/**
 * Internal termenum request executed directly against a specific index shard.
 */
public class ShardTermEnumRequest extends BroadcastShardRequest {

    private String field;
    private String pattern;
    private long nowInMillis;
    private AliasFilter filteringAliases;
    private boolean leadingWildcard;
    private boolean traillingWildcard;
    private boolean caseInsensitive;
    private boolean useRegexpSyntax;
    private boolean sortByPopularity;
    private int minShardDocFreq;
    private int size;
    private int timeout;

    public ShardTermEnumRequest(StreamInput in) throws IOException {
        super(in);
        filteringAliases = new AliasFilter(in);
        field = in.readString();
        pattern = in.readString();
        leadingWildcard = in.readBoolean();
        traillingWildcard = in.readBoolean();
        caseInsensitive = in.readBoolean();
        useRegexpSyntax = in.readBoolean();
        sortByPopularity = in.readBoolean();
        minShardDocFreq = in.readVInt();
        size = in.readVInt();
        timeout = in.readVInt();
        nowInMillis = in.readVLong();

    }

    public ShardTermEnumRequest(ShardId shardId, AliasFilter filteringAliases, TermEnumRequest request) {
        super(shardId, request);
        this.field = request.field();
        this.pattern = request.pattern();
        this.leadingWildcard = request.leadingWildcard();
        this.traillingWildcard = request.traillingWildcard();
        this.caseInsensitive = request.caseInsensitive();
        this.useRegexpSyntax = request.useRegexpSyntax();
        this.minShardDocFreq = request.minShardDocFreq();
        this.size = request.size();
        this.timeout = request.timeout();
        this.sortByPopularity = request.sortByPopularity();
        this.filteringAliases = Objects.requireNonNull(filteringAliases, "filteringAliases must not be null");
        this.nowInMillis = request.nowInMillis;
    }

    public String field() {
        return field;
    }

    public String pattern() {
        return pattern;
    }

    public AliasFilter filteringAliases() {
        return filteringAliases;
    }

    public long nowInMillis() {
        return this.nowInMillis;
    }

    public boolean leadingWildcard() {
        return leadingWildcard;
    }

    public boolean traillingWildcard() {
        return traillingWildcard;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    public boolean useRegexpSyntax() {
        return useRegexpSyntax;
    }

    public boolean sortByPopularity() {
        return sortByPopularity;
    }

    public int minShardDocFreq() {
        return minShardDocFreq;
    }

    public int size() {
        return size;
    }

    public int timeout() {
        return timeout;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        filteringAliases.writeTo(out);
        out.writeString(field);
        out.writeString(pattern);
        out.writeBoolean(leadingWildcard);
        out.writeBoolean(traillingWildcard);
        out.writeBoolean(caseInsensitive);
        out.writeBoolean(useRegexpSyntax);
        out.writeBoolean(sortByPopularity);
        out.writeVInt(minShardDocFreq);
        out.writeVInt(size);
        out.writeVInt(timeout);
        out.writeVLong(nowInMillis);
    }
}

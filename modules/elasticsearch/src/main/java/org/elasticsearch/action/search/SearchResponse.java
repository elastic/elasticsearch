/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.action.search;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facets.Facets;
import org.elasticsearch.search.internal.InternalSearchResponse;

import java.io.IOException;

import static org.elasticsearch.action.search.ShardSearchFailure.*;
import static org.elasticsearch.search.internal.InternalSearchResponse.*;

/**
 * A response of a search request.
 *
 * @author kimchy (shay.banon)
 */
public class SearchResponse implements ActionResponse, ToXContent {

    private InternalSearchResponse internalResponse;

    private String scrollId;

    private int totalShards;

    private int successfulShards;

    private ShardSearchFailure[] shardFailures;

    SearchResponse() {
    }

    public SearchResponse(InternalSearchResponse internalResponse, String scrollId, int totalShards, int successfulShards, ShardSearchFailure[] shardFailures) {
        this.internalResponse = internalResponse;
        this.scrollId = scrollId;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.shardFailures = shardFailures;
    }

    /**
     * The search hits.
     */
    public SearchHits hits() {
        return internalResponse.hits();
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return hits();
    }

    /**
     * The search facets.
     */
    public Facets facets() {
        return internalResponse.facets();
    }

    /**
     * The search facets.
     */
    public Facets getFacets() {
        return facets();
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int successfulShards() {
        return successfulShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int failedShards() {
        return totalShards - successfulShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return failedShards();
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] shardFailures() {
        return this.shardFailures;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return shardFailures;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String scrollId() {
        return scrollId;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        if (scrollId != null) {
            builder.field("_scrollId", scrollId);
        }
        builder.startObject("_shards");
        builder.field("total", totalShards());
        builder.field("successful", successfulShards());
        builder.field("failed", failedShards());

        if (shardFailures.length > 0) {
            builder.startArray("failures");
            for (ShardSearchFailure shardFailure : shardFailures) {
                builder.startObject();
                if (shardFailure.shard() != null) {
                    builder.field("index", shardFailure.shard().index());
                    builder.field("shard", shardFailure.shard().shardId());
                }
                builder.field("reason", shardFailure.reason());
                builder.endObject();
            }
            builder.endArray();
        }

        builder.endObject();
        internalResponse.toXContent(builder, params);
    }

    public static SearchResponse readSearchResponse(StreamInput in) throws IOException {
        SearchResponse response = new SearchResponse();
        response.readFrom(in);
        return response;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        internalResponse = readInternalSearchResponse(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = readShardSearchFailure(in);
            }
        }
        if (in.readBoolean()) {
            scrollId = in.readUTF();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        internalResponse.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }

        if (scrollId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(scrollId);
        }
    }
}

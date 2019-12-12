/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.Objects;

/**
 * A search response that contains partial results.
 */
public class PartialSearchResponse implements ToXContentFragment, Writeable {
    private final int totalShards;
    private final int successfulShards;
    private final int shardFailures;

    private final TotalHits totalHits;
    private InternalAggregations aggregations;

    public PartialSearchResponse(int totalShards) {
        this(totalShards, 0, 0, null, null);
    }

    public PartialSearchResponse(int totalShards, int successfulShards, int shardFailures,
                                 TotalHits totalHits, InternalAggregations aggregations) {
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.shardFailures = shardFailures;
        this.totalHits = totalHits;
        this.aggregations = aggregations;
    }

    public PartialSearchResponse(StreamInput in) throws IOException {
        this.totalShards = in.readVInt();
        this.successfulShards = in.readVInt();
        this.shardFailures = in.readVInt();
        this.totalHits = in.readBoolean() ? Lucene.readTotalHits(in) : null;
        this.aggregations = in.readOptionalWriteable(InternalAggregations::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(shardFailures);
        out.writeBoolean(totalHits != null);
        if (totalHits != null) {
            Lucene.writeTotalHits(out, totalHits);
        }
        out.writeOptionalWriteable(aggregations);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("is_partial", true);
        builder.field("total_shards", totalShards);
        builder.field("successful_shards", successfulShards);
        builder.field("shard_failures", shardFailures);
        if (totalHits != null) {
            builder.startObject(SearchHits.Fields.TOTAL);
            builder.field("value", totalHits.value);
            builder.field("relation", totalHits.relation == TotalHits.Relation.EQUAL_TO ? "eq" : "gte");
            builder.endObject();
        }
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }

    /**
     *  The total number of shards the search should executed on.
     */
    public int getTotalShards() {
        return totalShards;
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
    public int getShardFailures() {
        return shardFailures;
    }

    /**
     * Return the partial {@link TotalHits} computed from the shards that
     * completed the query phase.
     */
    public TotalHits getTotalHits() {
        return totalHits;
    }

    /**
     * Return the partial {@link InternalAggregations} computed from the shards that
     * completed the query phase.
     */
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PartialSearchResponse that = (PartialSearchResponse) o;
        return totalShards == that.totalShards &&
            successfulShards == that.successfulShards &&
            shardFailures == that.shardFailures &&
            Objects.equals(totalHits, that.totalHits) &&
            Objects.equals(aggregations, that.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalShards, successfulShards, shardFailures, totalHits, aggregations);
    }
}

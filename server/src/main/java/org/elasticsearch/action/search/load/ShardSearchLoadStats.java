/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search.load;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Objects;

/**
 * ShardSearchLoadStats class represents the statistics of a shard in an index.
 * It contains information such as the index name, shard ID, allocation ID, and EWMA rate.
 */
public class ShardSearchLoadStats implements Writeable {

    private final String indexName;

    private final Integer shardId;

    private final String allocationId;

    private final Double searchLoad;

    /**
     * Constructor to create a ShardStats object from a StreamInput.
     *
     * @param in the StreamInput to read from
     * @throws IOException if an I/O error occurs
     */
    public ShardSearchLoadStats(StreamInput in) throws IOException {
        assert Transports.assertNotTransportThread("O(#shards) work must always fork to an appropriate executor");
        this.indexName = in.readString();
        this.shardId = in.readVInt();
        this.allocationId = in.readString();
        this.searchLoad = in.readDouble();
    }

    /**
     * Constructor to create a ShardStats object with the given parameters.
     *
     * @param indexName   the name of the index
     * @param shardId     the ID of the shard
     * @param allocationId the allocation ID of the shard
     * @param searchLoad the search load of the shard
     */
    public ShardSearchLoadStats(String indexName, Integer shardId, String allocationId, Double searchLoad) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.searchLoad = searchLoad;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardSearchLoadStats that = (ShardSearchLoadStats) o;
        return Objects.equals(indexName, that.indexName)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(allocationId, that.allocationId)
            && Objects.equals(searchLoad, that.searchLoad);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexName, shardId, allocationId, searchLoad);
    }

    /**
     * Returns the index name of the shard.
     *
     * @return the index name
     */
    public String getIndexName() {
        return this.indexName;
    }

    /**
     * Returns the shard ID of the shard.
     *
     * @return the shard ID
     */
    public Integer getShardId() {
        return this.shardId;
    }

    /**
     * Returns the allocation ID of the shard.
     *
     * @return the allocation ID
     */
    public String getAllocationId() {
        return this.allocationId;
    }

    /**
     * Returns the search load of the shard.
     *
     * @return the search load as a Double
     */
    public Double getSearchLoad() {
        return this.searchLoad;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeVInt(shardId);
        out.writeString(allocationId);
        out.writeDouble(searchLoad);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats.rrc;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.transport.Transports;

import java.io.IOException;
import java.util.Objects;

public class ShardStats implements Writeable {

    private final String indexName;

    private final Integer shardId;

    private final String allocationId;

    private final Double movingAverage;

    public ShardStats(StreamInput in) throws IOException {
        assert Transports.assertNotTransportThread("O(#shards) work must always fork to an appropriate executor");
        this.indexName = in.readString();
        this.shardId = in.readVInt();
        this.allocationId = in.readString();
        this.movingAverage = in.readDouble();
    }

    public ShardStats(String indexName, Integer shardId, String allocationId, Double ewma) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.movingAverage = ewma;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardStats that = (ShardStats) o;
        return Objects.equals(indexName, that.indexName)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(allocationId, that.allocationId)
            && Objects.equals(movingAverage, that.movingAverage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            indexName,
            shardId,
            allocationId,
            movingAverage
        );
    }

    public String getIndexName() {
        return this.indexName;
    }

    public Integer getShardId() {
        return this.shardId;
    }

    public String getAllocationId() {
        return this.allocationId;
    }

    public Double getMovingAverage() {
        return this.movingAverage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
         out.writeString(indexName);
         out.writeVInt(shardId);
         out.writeString(allocationId);
         out.writeDouble(movingAverage);
    }
}

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
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ShardStatsRRC implements Writeable, ToXContentFragment {

    private final String indexName;

    private final Integer shardId;

    private final String allocationId;

    private final Double ewma;

    public ShardStatsRRC(StreamInput in) throws IOException {
        assert Transports.assertNotTransportThread("O(#shards) work must always fork to an appropriate executor");
        this.indexName = in.readString();
        this.shardId = in.readVInt();
        this.allocationId = in.readString();
        this.ewma = in.readDouble();
    }

    public ShardStatsRRC(String indexName, Integer shardId, String allocationId, Double ewma) {
        this.indexName = indexName;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.ewma = ewma;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardStatsRRC that = (ShardStatsRRC) o;
        return Objects.equals(indexName, that.indexName)
            && Objects.equals(shardId, that.shardId)
            && Objects.equals(allocationId, that.allocationId)
            && Objects.equals(ewma, that.ewma);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            indexName,
            shardId,
            allocationId,
            ewma
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

    public String getShardName() {
        return this.indexName + "_" + this.shardId + "_" + this.allocationId;
    }

    public Double getEWMA() {
        return this.ewma;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
         out.writeString(indexName);
         out.writeVInt(shardId);
         out.writeString(allocationId);
         out.writeDouble(ewma);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.ROUTING)
            .field(Fields.INDEX_NAME, indexName)
            .field(Fields.SHARD_ID, shardId)
            .field(Fields.ALLOCATION_ID, allocationId)
            .field(Fields.EWMA, ewma)
            .endObject();
        return builder;
    }

    static final class Fields {
        static final String ROUTING = "routing";
        static final String INDEX_NAME = "index_name";
        static final String SHARD_ID = "shard_id";
        static final String ALLOCATION_ID = "allocation_id";
        static final String EWMA = "ewma";
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * The RRF results for a single shard.
 */
public class RRFRankShardResult implements RankShardResult {

    /**
     * The number of queries used for RRF.
     */
    public final int queryCount;

    /**
     * The result set of RRF docs for the shard.
     */
    public final RRFRankDoc[] rrfRankDocs;

    public RRFRankShardResult(int queryCount, RRFRankDoc[] rrfRankDocs) {
        this.queryCount = queryCount;
        this.rrfRankDocs = Objects.requireNonNull(rrfRankDocs);
    }

    public RRFRankShardResult(StreamInput in) throws IOException {
        queryCount = in.readVInt();
        rrfRankDocs = in.readArray(RRFRankDoc::new, RRFRankDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return RRFRankPlugin.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryCount);
        out.writeArray(rrfRankDocs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RRFRankShardResult that = (RRFRankShardResult) o;
        return queryCount == that.queryCount && Arrays.equals(rrfRankDocs, that.rrfRankDocs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(queryCount);
        result = 31 * result + Arrays.hashCode(rrfRankDocs);
        return result;
    }

    @Override
    public String toString() {
        return "RRFRankShardResult{" + "queryCount=" + queryCount + ", rrfRankDocs=" + Arrays.toString(rrfRankDocs) + '}';
    }
}

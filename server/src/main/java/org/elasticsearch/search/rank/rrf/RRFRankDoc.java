/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * {@code RRFRankDoc} supports additional ranking information
 * required for RRF.
 */
public class RRFRankDoc extends RankDoc {

    public static final int NO_RANK = -1;

    /**
     * If this document has been ranked, this is its final
     * rrf ranking from all the result sets.
     */
    public int rank;

    /**
     * The position within each result set per query. The length
     * of {@code positions} is the number of queries that are part
     * of rrf ranking. If a document isn't part of a result set for a
     * specific query then the position is {@link RRFRankDoc#NO_RANK}.
     * This allows for a direct association with each query.
     */
    public final int[] positions;

    /**
     * The score for each result set per query. The length
     * of {@code positions} is the number of queries that are part
     * of rrf ranking. If a document isn't part of a result set for a
     * specific query then the score is {@code 0f}. This allows for a
     * direct association with each query.
     */
    public final float[] scores;

    public RRFRankDoc(int doc, int shardIndex, int queryCount) {
        super(doc, 0f, shardIndex);
        positions = new int[queryCount];
        Arrays.fill(positions, NO_RANK);
        scores = new float[queryCount];
    }

    public RRFRankDoc(StreamInput in) throws IOException {
        super(in);
        rank = in.readVInt();
        positions = in.readIntArray();
        scores = in.readFloatArray();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rank);
        out.writeIntArray(positions);
        out.writeFloatArray(scores);
    }

    @Override
    public String getWriteableName() {
        return RRFRankBuilder.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    public boolean doEquals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RRFRankDoc that = (RRFRankDoc) o;
        return rank == that.rank && Arrays.equals(positions, that.positions) && Arrays.equals(scores, that.scores);
    }

    @Override
    public int doHashCode() {
        int result = Objects.hash(rank);
        result = 31 * result + Arrays.hashCode(positions);
        result = 31 * result + Arrays.hashCode(scores);
        return result;
    }

    @Override
    public String toString() {
        return "RRFRankDoc{"
            + "rank="
            + rank
            + ", positions="
            + Arrays.toString(positions)
            + ", scores="
            + Arrays.toString(scores)
            + ", score="
            + score
            + ", doc="
            + doc
            + ", shardIndex="
            + shardIndex
            + '}';
    }
}

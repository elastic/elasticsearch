/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * {@code RRFRankDoc} supports additional ranking information
 * required for RRF.
 */
public class RRFRankDoc extends RankDoc {

    static final String NAME = "rrf_rank_doc";

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
    public Explanation explain() {
        // ideally we'd need access to the rank constant to provide score info for this one
        int queries = positions.length;
        Explanation[] details = new Explanation[queries];
        for (int i = 0; i < queries; i++) {
            final String queryIndex = "at index [" + i + "]";
            if (positions[i] == RRFRankDoc.NO_RANK) {
                final String description = "rrf score: [0], result not found in query " + queryIndex;
                details[i] = Explanation.noMatch(description);
            } else {
                final int rank = positions[i] + 1;
                details[i] = Explanation.match(rank, "rank [" + (rank) + "] in query " + queryIndex);
            }
        }
        return Explanation.match(
            score,
            "rrf score: ["
                + score
                + "] computed for initial ranks "
                + Arrays.toString(Arrays.stream(positions).map(x -> x + 1).toArray())
                + "] as sum of [1 / (rank + rankConstant)] for each query",
            details
        );
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rank);
        out.writeIntArray(positions);
        out.writeFloatArray(scores);
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        RRFRankDoc rrfrd = (RRFRankDoc) rd;
        return Arrays.equals(positions, rrfrd.positions) && Arrays.equals(scores, rrfrd.scores);
    }

    @Override
    public int doHashCode() {
        int result = Arrays.hashCode(positions);
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("positions", positions);
        builder.field("scores", scores);
    }
}

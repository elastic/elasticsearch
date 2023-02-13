/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

public class RankResults {

    public static class RankResult implements Writeable {
        public final int doc;
        public final int shard;
        public int rank;
        public float score;
        public final int[] positions;
        public final float[] scores;

        public RankResult(int doc, int shard, int size) {
            this.doc = doc;
            this.shard = shard;
            this.positions = new int[size];
            this.scores = new float[size];
        }

        public RankResult(StreamInput in) throws IOException {
            doc = in.readVInt();
            shard = in.readVInt();
            rank = in.readVInt();
            score = in.readFloat();
            positions = in.readIntArray();
            scores = in.readFloatArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(doc);
            out.writeVInt(shard);
            out.writeVInt(rank);
            out.writeFloat(score);
            out.writeIntArray(positions);
            out.writeFloatArray(scores);
        }
    }

    private final List<RankResult> rankResults;

    public RankResults(List<RankResult> rankResults) {
        this.rankResults = rankResults;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;

public class LinearRankDoc extends RankDoc {

    public float[] weights;
    public float[] scores;
    public String[] normalizers;

    public LinearRankDoc(int doc, float score, int shardIndex, int queriesCount) {
        super(doc, score, shardIndex);
        this.weights = new float[queriesCount];
        this.scores = new float[queriesCount];
        Arrays.fill(scores, 0f);
        this.normalizers = new String[queriesCount];
    }

    public LinearRankDoc(StreamInput in) throws IOException {
        super(in.readVInt(), in.readFloat(), in.readVInt());
        weights = in.readFloatArray();
        scores = in.readFloatArray();
        normalizers = in.readStringArray();
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        Explanation[] details = new Explanation[sources.length];
        for (int i = 0; i < sources.length; i++) {
            final String queryAlias = queryNames[i] == null ? "" : " [" + queryNames[i] + "]";
            final String queryIdentifier = "at index [" + i + "]" + queryAlias;
            if (scores[i] > 0) {
                details[i] = Explanation.match(
                    weights[i] * scores[i],
                    "weighted score: ["
                        + weights[i] * scores[i]
                        + "] in query "
                        + queryIdentifier
                        + " computed as ["
                        + weights[i]
                        + " * "
                        + scores[i]
                        + "]"
                        + " using score normalizer ["
                        + normalizers[i]
                        + "]"
                        + " for original matching query with score:",
                    sources[i]
                );
            } else {
                final String description = "weighted score: [0], result not found in query " + queryIdentifier;
                details[i] = Explanation.noMatch(description);
            }
        }
        return Explanation.match(
            score,
            "weighted linear combination score: ["
                + score
                + "] computed for normalized scores "
                + Arrays.toString(scores)
                + " and weights "
                + Arrays.toString(weights)
                + "] as sum of (weight[i] * score[i]) for each query.",
            details
        );
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeFloatArray(weights);
        out.writeFloatArray(scores);
        out.writeStringArray(normalizers);
    }
}

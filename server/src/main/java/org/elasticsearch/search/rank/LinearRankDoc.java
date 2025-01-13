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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class LinearRankDoc extends RankDoc {

    public float[] weights;
    public float[] normalizedScores;
    public String[] normalizers;

    public LinearRankDoc(int doc, float score, int shardIndex, int queriesCount) {
        super(doc, score, shardIndex);
        this.weights = new float[queriesCount];
        this.normalizedScores = new float[queriesCount];
        this.normalizers = new String[queriesCount];
    }

    public LinearRankDoc(StreamInput in) throws IOException {
        super(in.readVInt(), in.readFloat(), in.readVInt());
        weights = in.readFloatArray();
        normalizedScores = in.readFloatArray();
        normalizers = in.readStringArray();
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        Explanation[] details = new Explanation[sources.length];
        for (int i = 0; i < sources.length; i++) {
            final String queryAlias = queryNames[i] == null ? "" : " [" + queryNames[i] + "]";
            final String queryIdentifier = "at index [" + i + "]" + queryAlias;
            if (normalizedScores[i] > 0) {
                details[i] = Explanation.match(
                    weights[i] * normalizedScores[i],
                    "weighted score: ["
                        + weights[i] * normalizedScores[i]
                        + "] in query "
                        + queryIdentifier
                        + " computed as ["
                        + weights[i]
                        + " * "
                        + normalizedScores[i]
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
                + Arrays.toString(normalizedScores)
                + " and weights "
                + Arrays.toString(weights)
                + "] as sum of (weight[i] * score[i]) for each query.",
            details
        );
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeFloatArray(weights);
        out.writeFloatArray(normalizedScores);
        out.writeStringArray(normalizers);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("weights", weights);
        builder.field("normalizedScores", normalizedScores);
        builder.field("normalizers", normalizers);
    }

    @Override
    public boolean doEquals(RankDoc rd) {
        LinearRankDoc lrd = (LinearRankDoc) rd;
        return Arrays.equals(weights, lrd.weights)
            && Arrays.equals(normalizedScores, lrd.normalizedScores)
            && Arrays.equals(normalizers, lrd.normalizers);
    }

    @Override
    public int doHashCode() {
        int result = Objects.hash(Arrays.hashCode(weights), Arrays.hashCode(normalizedScores) + Arrays.hashCode(normalizers));
        return 31 * result;
    }
}

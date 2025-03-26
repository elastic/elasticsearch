/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.Explanation;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.xpack.rank.linear.LinearRetrieverBuilder.DEFAULT_SCORE;
import static org.elasticsearch.xpack.rank.linear.LinearRetrieverComponent.DEFAULT_NORMALIZER;
import static org.elasticsearch.xpack.rank.linear.LinearRetrieverComponent.DEFAULT_WEIGHT;

public class LinearRankDoc extends RankDoc {

    public static final String NAME = "linear_rank_doc";

    final float[] weights;
    final String[] normalizers;
    public float[] normalizedScores;

    public LinearRankDoc(int doc, float score, int shardIndex) {
        super(doc, score, shardIndex);
        this.weights = null;
        this.normalizers = null;
    }

    public LinearRankDoc(int doc, float score, int shardIndex, float[] weights, String[] normalizers) {
        super(doc, score, shardIndex);
        this.weights = weights;
        this.normalizers = normalizers;
    }

    public LinearRankDoc(StreamInput in) throws IOException {
        super(in);
        weights = in.readOptionalFloatArray();
        normalizedScores = in.readOptionalFloatArray();
        normalizers = in.readOptionalStringArray();
    }

    @Override
    public Explanation explain(Explanation[] sources, String[] queryNames) {
        assert normalizedScores != null && weights != null && normalizers != null;
        assert normalizedScores.length == sources.length;

        Explanation[] details = new Explanation[sources.length];
        for (int i = 0; i < sources.length; i++) {
            final String queryAlias = queryNames[i] == null ? "" : " [" + queryNames[i] + "]";
            final String queryIdentifier = "at index [" + i + "]" + queryAlias;
            final float weight = weights == null ? DEFAULT_WEIGHT : weights[i];
            final float normalizedScore = normalizedScores == null ? DEFAULT_SCORE : normalizedScores[i];
            final String normalizer = normalizers == null ? DEFAULT_NORMALIZER.getName() : normalizers[i];
            if (normalizedScore > 0) {
                details[i] = Explanation.match(
                    weight * normalizedScore,
                    "weighted score: ["
                        + weight * normalizedScore
                        + "] in query "
                        + queryIdentifier
                        + " computed as ["
                        + weight
                        + " * "
                        + normalizedScore
                        + "]"
                        + " using score normalizer ["
                        + normalizer
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
                + (weights == null ? "" : " and weights " + Arrays.toString(weights))
                + " as sum of (weight[i] * score[i]) for each query.",
            details
        );
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalFloatArray(weights);
        out.writeOptionalFloatArray(normalizedScores);
        out.writeOptionalStringArray(normalizers);
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (weights != null) {
            builder.field("weights", weights);
        }
        if (normalizedScores != null) {
            builder.field("normalizedScores", normalizedScores);
        }
        if (normalizers != null) {
            builder.field("normalizers", normalizers);
        }
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
        int result = Objects.hash(Arrays.hashCode(weights), Arrays.hashCode(normalizedScores), Arrays.hashCode(normalizers));
        return 31 * result;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.LINEAR_RETRIEVER_SUPPORT;
    }
}

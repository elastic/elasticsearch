/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.eval;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Recall, cost, mapping and shard facts from one rank-vectors evaluation. */
public final class RankVectorsEvalResponse extends ActionResponse implements ToXContentObject {

    public record Hit(String index, String id, float score) implements Writeable {
        Hit(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), in.readFloat());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeString(id);
            out.writeFloat(score);
        }
    }

    private final double candidateRecall;
    private final double finalRecall;
    private final int candidateIntersection;
    private final int finalIntersection;
    private final int baselineCount;
    private final int candidateCount;
    private final int finalCount;
    private final int baselineShortfall;
    private final boolean cutoffTie;
    private final long exactTookMillis;
    private final long candidateTookMillis;
    private final long finalTookMillis;
    private final long documentCount;
    private final long exactDotProducts;
    private final long candidateBudget;
    private final long rescoreBudget;
    private final int primaryShards;
    private final String strategy;
    private final String encodingProfile;
    private final int encodingVersion;
    private final List<Hit> exactHits;
    private final List<Hit> finalHits;

    public RankVectorsEvalResponse(
        double candidateRecall,
        double finalRecall,
        int candidateIntersection,
        int finalIntersection,
        int baselineCount,
        int candidateCount,
        int finalCount,
        int baselineShortfall,
        boolean cutoffTie,
        long exactTookMillis,
        long candidateTookMillis,
        long finalTookMillis,
        long documentCount,
        long exactDotProducts,
        long candidateBudget,
        long rescoreBudget,
        int primaryShards,
        String strategy,
        String encodingProfile,
        int encodingVersion,
        List<Hit> exactHits,
        List<Hit> finalHits
    ) {
        this.candidateRecall = candidateRecall;
        this.finalRecall = finalRecall;
        this.candidateIntersection = candidateIntersection;
        this.finalIntersection = finalIntersection;
        this.baselineCount = baselineCount;
        this.candidateCount = candidateCount;
        this.finalCount = finalCount;
        this.baselineShortfall = baselineShortfall;
        this.cutoffTie = cutoffTie;
        this.exactTookMillis = exactTookMillis;
        this.candidateTookMillis = candidateTookMillis;
        this.finalTookMillis = finalTookMillis;
        this.documentCount = documentCount;
        this.exactDotProducts = exactDotProducts;
        this.candidateBudget = candidateBudget;
        this.rescoreBudget = rescoreBudget;
        this.primaryShards = primaryShards;
        this.strategy = strategy;
        this.encodingProfile = encodingProfile;
        this.encodingVersion = encodingVersion;
        this.exactHits = List.copyOf(exactHits);
        this.finalHits = List.copyOf(finalHits);
    }

    public RankVectorsEvalResponse(StreamInput in) throws IOException {
        candidateRecall = in.readDouble();
        finalRecall = in.readDouble();
        candidateIntersection = in.readVInt();
        finalIntersection = in.readVInt();
        baselineCount = in.readVInt();
        candidateCount = in.readVInt();
        finalCount = in.readVInt();
        baselineShortfall = in.readVInt();
        cutoffTie = in.readBoolean();
        exactTookMillis = in.readVLong();
        candidateTookMillis = in.readVLong();
        finalTookMillis = in.readVLong();
        documentCount = in.readVLong();
        exactDotProducts = in.readVLong();
        candidateBudget = in.readVLong();
        rescoreBudget = in.readVLong();
        primaryShards = in.readVInt();
        strategy = in.readString();
        encodingProfile = in.readString();
        encodingVersion = in.readVInt();
        exactHits = in.readCollectionAsList(Hit::new);
        finalHits = in.readCollectionAsList(Hit::new);
    }

    public double candidateRecall() {
        return candidateRecall;
    }

    public double finalRecall() {
        return finalRecall;
    }

    public int baselineCount() {
        return baselineCount;
    }

    public int candidateCount() {
        return candidateCount;
    }

    public int primaryShards() {
        return primaryShards;
    }

    public List<Hit> exactHits() {
        return exactHits;
    }

    public List<Hit> finalHits() {
        return finalHits;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(candidateRecall);
        out.writeDouble(finalRecall);
        out.writeVInt(candidateIntersection);
        out.writeVInt(finalIntersection);
        out.writeVInt(baselineCount);
        out.writeVInt(candidateCount);
        out.writeVInt(finalCount);
        out.writeVInt(baselineShortfall);
        out.writeBoolean(cutoffTie);
        out.writeVLong(exactTookMillis);
        out.writeVLong(candidateTookMillis);
        out.writeVLong(finalTookMillis);
        out.writeVLong(documentCount);
        out.writeVLong(exactDotProducts);
        out.writeVLong(candidateBudget);
        out.writeVLong(rescoreBudget);
        out.writeVInt(primaryShards);
        out.writeString(strategy);
        out.writeString(encodingProfile);
        out.writeVInt(encodingVersion);
        out.writeCollection(exactHits);
        out.writeCollection(finalHits);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("candidate_recall", candidateRecall);
        builder.field("final_recall", finalRecall);
        builder.startObject("intersections");
        builder.field("candidates", candidateIntersection);
        builder.field("final", finalIntersection);
        builder.endObject();
        builder.startObject("counts");
        builder.field("baseline", baselineCount);
        builder.field("candidates", candidateCount);
        builder.field("final", finalCount);
        builder.field("baseline_shortfall", baselineShortfall);
        builder.endObject();
        builder.startObject("cutoff");
        builder.field("exact_tie", cutoffTie);
        builder.field("recall_ambiguous", cutoffTie);
        builder.endObject();
        builder.startObject("timings_ms");
        builder.field("exact", exactTookMillis);
        builder.field("candidates", candidateTookMillis);
        builder.field("final", finalTookMillis);
        builder.endObject();
        builder.startObject("work");
        builder.field("documents", documentCount);
        builder.field("exact_dot_products_upper_bound", exactDotProducts);
        builder.field("candidate_budget", candidateBudget);
        builder.field("exact_rescore_budget", rescoreBudget);
        builder.endObject();
        builder.startObject("field");
        builder.field("strategy", strategy);
        builder.field("encoding_profile", encodingProfile);
        builder.field("encoding_version", encodingVersion);
        builder.endObject();
        builder.startObject("shards");
        builder.field("primaries", primaryShards);
        builder.field("candidate_budget_per_primary", candidateBudget / primaryShards);
        builder.endObject();
        hits(builder, "exact_hits", exactHits);
        hits(builder, "final_hits", finalHits);
        return builder.endObject();
    }

    private static void hits(XContentBuilder builder, String name, List<Hit> hits) throws IOException {
        builder.startArray(name);
        for (Hit hit : hits) {
            builder.map(Map.of("_index", hit.index(), "_id", hit.id(), "_score", hit.score()));
        }
        builder.endArray();
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankShardContext;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.rank.random.RandomRankRetrieverBuilder.FIELD_FIELD;
import static org.elasticsearch.xpack.inference.rank.random.RandomRankRetrieverBuilder.SEED_FIELD;

/**
 * A {@code RankBuilder} that performs reranking with random scores, used for testing.
 */
public class RandomRankBuilder extends RankBuilder {

    public static final String NAME = "random_reranker";

    static final ConstructingObjectParser<RandomRankBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        Integer rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (Integer) args[0];
        String field = (String) args[1];
        Integer seed = (Integer) args[2];

        return new RandomRankBuilder(rankWindowSize, field, seed);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), SEED_FIELD);
    }

    private final String field;
    private final Integer seed;

    public RandomRankBuilder(int rankWindowSize, String field, Integer seed) {
        super(rankWindowSize);

        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("field is required");
        }

        this.field = field;
        this.seed = seed;
    }

    public RandomRankBuilder(StreamInput in) throws IOException {
        super(in);
        // rankWindowSize deserialization is handled by the parent class RankBuilder
        this.field = in.readString();
        this.seed = in.readOptionalInt();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_16_0;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        // rankWindowSize serialization is handled by the parent class RankBuilder
        out.writeString(field);
        out.writeOptionalInt(seed);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        // rankWindowSize serialization is handled by the parent class RankBuilder
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (seed != null) {
            builder.field(SEED_FIELD.getPreferredName(), seed);
        }
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
        if (scoreDoc == null) {
            return baseExplanation;
        }
        if (false == baseExplanation.isMatch()) {
            return baseExplanation;
        }

        assert scoreDoc instanceof RankFeatureDoc : "ScoreDoc is not an instance of RankFeatureDoc";
        RankFeatureDoc rankFeatureDoc = (RankFeatureDoc) scoreDoc;

        return Explanation.match(
            rankFeatureDoc.score,
            "rank after reranking: [" + rankFeatureDoc.rank + "] using seed [" + seed + "] with score: [" + rankFeatureDoc.score + "]",
            baseExplanation
        );
    }

    @Override
    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return new RerankingQueryPhaseRankShardContext(queries, rankWindowSize());
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return new RerankingQueryPhaseRankCoordinatorContext(rankWindowSize());
    }

    @Override
    public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return new RerankingRankFeaturePhaseRankShardContext(field);
    }

    @Override
    public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return new RandomRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize(), seed);
    }

    public String field() {
        return field;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        RandomRankBuilder that = (RandomRankBuilder) other;
        return Objects.equals(field, that.field) && Objects.equals(seed, that.seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, seed);
    }
}

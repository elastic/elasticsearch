/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilderWrapper;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.rank.linear.LinearRetrieverBuilder;
import org.elasticsearch.xpack.rank.linear.MinMaxScoreNormalizer;
import org.elasticsearch.xpack.rank.linear.ScoreNormalizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.search.retriever.CompoundRetrieverBuilder.RANK_WINDOW_SIZE_FIELD;

public class HybridRetrieverBuilder extends RetrieverBuilderWrapper<HybridRetrieverBuilder> {
    public static final String NAME = "hybrid";
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField QUERY_FIELD = new ParseField("query");

    private final List<String> fields;
    private final String query;
    private final int rankWindowSize;

    public HybridRetrieverBuilder(List<String> fields, String query, int rankWindowSize) {
        this(
            fields == null ? List.of() : List.copyOf(fields),
            query,
            rankWindowSize,
            new LinearRetrieverBuilder(
                generateInnerRetrievers(fields, query),
                rankWindowSize,
                generateWeights(fields),
                generateScoreNormalizers(fields)
            )
        );
    }

    private HybridRetrieverBuilder(List<String> fields, String query, int rankWindowSize, RetrieverBuilder retrieverBuilder) {
        super(retrieverBuilder);
        this.fields = fields;
        this.query = query;
        this.rankWindowSize = rankWindowSize;
    }

    @Override
    protected HybridRetrieverBuilder clone(RetrieverBuilder sub) {
        return new HybridRetrieverBuilder(fields, query, rankWindowSize, sub);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELDS_FIELD.getPreferredName(), fields);
        builder.field(QUERY_FIELD.getPreferredName(), query);
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    protected boolean doEquals(Object o) {
        // TODO: Check rankWindowSize? It should be checked by the wrapped retriever.
        HybridRetrieverBuilder that = (HybridRetrieverBuilder) o;
        return Objects.equals(fields, that.fields) && Objects.equals(query, that.query) && super.doEquals(o);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fields, query, super.doHashCode());
    }

    private static List<CompoundRetrieverBuilder.RetrieverSource> generateInnerRetrievers(List<String> fields, String query) {
        if (fields == null) {
            return List.of();
        }

        List<CompoundRetrieverBuilder.RetrieverSource> innerRetrievers = new ArrayList<>(fields.size());
        for (String field : fields) {
            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(field, query);
            innerRetrievers.add(new CompoundRetrieverBuilder.RetrieverSource(new StandardRetrieverBuilder(matchQueryBuilder), null));
        }

        return innerRetrievers;
    }

    private static float[] generateWeights(List<String> fields) {
        if (fields == null) {
            return new float[0];
        }

        // TODO: Parse field strings for weights
        float[] weights = new float[fields.size()];
        Arrays.fill(weights, 1.0f);
        return weights;
    }

    private static ScoreNormalizer[] generateScoreNormalizers(List<String> fields) {
        if (fields == null) {
            return new ScoreNormalizer[0];
        }

        ScoreNormalizer[] scoreNormalizers = new ScoreNormalizer[fields.size()];
        Arrays.fill(scoreNormalizers, MinMaxScoreNormalizer.INSTANCE);
        return scoreNormalizers;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@code RetrieverBuilder} for randomly scoring a set of documents using the {@code RandomRankBuilder}
 */
public class RandomRankRetrieverBuilder extends RetrieverBuilder {

    public static final NodeFeature RANDOM_RERANKER_RETRIEVER_SUPPORTED = new NodeFeature("random_reranker_retriever_supported");

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");
    public static final ParseField SEED_FIELD = new ParseField("seed");

    public static final ConstructingObjectParser<RandomRankRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(RandomRankBuilder.NAME, args -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            String field = (String) args[1];
            int rankWindowSize = args[2] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[2];
            Integer seed = (Integer) args[3];

            return new RandomRankRetrieverBuilder(retrieverBuilder, field, rankWindowSize, seed);
        });

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> p.namedObject(RetrieverBuilder.class, n, c), RETRIEVER_FIELD);
        PARSER.declareString(optionalConstructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), SEED_FIELD);

        RetrieverBuilder.declareBaseParserFields(RandomRankBuilder.NAME, PARSER);
    }

    public static RandomRankRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(RANDOM_RERANKER_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + RandomRankBuilder.NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    private final RetrieverBuilder retrieverBuilder;
    private final String field;
    private final int rankWindowSize;
    private final Integer seed;

    public RandomRankRetrieverBuilder(RetrieverBuilder retrieverBuilder, String field, int rankWindowSize, Integer seed) {
        this.retrieverBuilder = retrieverBuilder;
        this.field = field;
        this.rankWindowSize = rankWindowSize;
        this.seed = seed;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        return retrieverBuilder.topDocsQuery();
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        retrieverBuilder.extractToSearchSourceBuilder(searchSourceBuilder, compoundUsed);

        // Combining with other rank builder (such as RRF) is not supported
        if (searchSourceBuilder.rankBuilder() != null) {
            throw new IllegalArgumentException("random rank builder cannot be combined with other rank builders");
        }

        searchSourceBuilder.rankBuilder(new RandomRankBuilder(this.rankWindowSize, this.field, this.seed));
    }

    @Override
    public String getName() {
        return RandomRankBuilder.NAME;
    }

    public int rankWindowSize() {
        return rankWindowSize;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName());
        builder.startObject();
        builder.field(retrieverBuilder.getName(), retrieverBuilder);
        builder.endObject();
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        if (seed != null) {
            builder.field(SEED_FIELD.getPreferredName(), seed);
        }
    }

    @Override
    protected boolean doEquals(Object other) {
        RandomRankRetrieverBuilder that = (RandomRankRetrieverBuilder) other;
        return Objects.equals(retrieverBuilder, that.retrieverBuilder)
            && Objects.equals(field, that.field)
            && Objects.equals(rankWindowSize, that.rankWindowSize)
            && Objects.equals(seed, that.seed);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(retrieverBuilder, field, rankWindowSize, seed);
    }
}

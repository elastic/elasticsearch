/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankCoordinatorContext;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The builder to support RRF. Adds user-defined parameters for window size and rank constant.
 */
public class RRFRankBuilder extends RankBuilder<RRFRankBuilder> {

    public static final String NAME = "rrf";

    public static final int DEFAULT_RANK_CONSTANT = 60;

    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    private static final ConstructingObjectParser<RRFRankBuilder, Void> PARSER = new ConstructingObjectParser<>("rrf", args -> {
        RRFRankBuilder builder = new RRFRankBuilder();
        builder.windowSize(args[0] == null ? DEFAULT_WINDOW_SIZE : (int) args[0]);
        builder.rankConstant(args[1] == null ? DEFAULT_RANK_CONSTANT : (int) args[1]);
        return builder;
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
    }

    public static RRFRankBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    protected int rankConstant = DEFAULT_RANK_CONSTANT;

    public RRFRankBuilder() {}

    public RRFRankBuilder(StreamInput in) throws IOException {
        super(in);
        rankConstant = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rankConstant);
    }

    /**
     * Additional validation for RRF based on window size and rank constant.
     */
    @Override
    public ActionRequestValidationException validate(ActionRequestValidationException validationException, SearchSourceBuilder source) {
        if (source.size() > windowSize) {
            validationException = addValidationError(
                "[window_size] must be greater than or equal to [size] for [rrf]",
                validationException
            );
        }
        if (rankConstant < 1) {
            validationException = addValidationError("[rank_constant] must be greater than [0] for [rrf]", validationException);
        }
        if (source.knnSearch().isEmpty() || source.query() == null && source.knnSearch().size() < 2) {
            validationException = addValidationError("[rrf] requires a minimum of [2] queries", validationException);
        }

        return validationException;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    public RRFRankBuilder rankConstant(int rankConstant) {
        this.rankConstant = rankConstant;
        return this;
    }

    public int rankConstant() {
        return rankConstant;
    }

    /**
     * RRF builds a simple boolean disjunction for all queries when
     * building aggregations and suggesters.
     */
    @Override
    public QueryBuilder searchQuery(List<QueryBuilder> queryBuilders) {
        BoolQueryBuilder searchQuery = new BoolQueryBuilder();
        for (QueryBuilder queryBuilder : queryBuilders) {
            searchQuery.should(queryBuilder);
        }

        return searchQuery;
    }

    @Override
    public RankShardContext build(List<Query> queries, int size, int from) {
        return new RRFRankShardContext(queries, size, from, windowSize, rankConstant);
    }

    @Override
    public RankCoordinatorContext build(int size, int from) {
        return new RRFRankCoordinatorContext(size, from, windowSize, rankConstant);
    }

    @Override
    protected boolean doEquals(RRFRankBuilder other) {
        return Objects.equals(rankConstant, other.rankConstant);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rankConstant);
    }
}

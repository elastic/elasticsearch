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
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankContext;
import org.elasticsearch.search.rank.RankContextBuilder;
import org.elasticsearch.search.rank.RankShardContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RRFRankContextBuilder extends RankContextBuilder {

    public static final String NAME = "rrf";

    public static final int RANK_CONSTANT_DEFAULT = 20;
    public static final int WINDOW_SIZE_DEFAULT = 10;

    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    private static final ConstructingObjectParser<RRFRankContextBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "rrf",
        args -> new RRFRankContextBuilder().windowSize(args[0] == null ? WINDOW_SIZE_DEFAULT : (int) args[0])
            .rankConstant(args[1] == null ? RANK_CONSTANT_DEFAULT : (int) args[1])
    );

    static {
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
    }

    public static RRFRankContextBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
        builder.endArray();

        return builder;
    }

    private int windowSize;
    private int rankConstant;

    public RRFRankContextBuilder() {}

    public RRFRankContextBuilder(StreamInput in) throws IOException {
        super(in);
        windowSize = in.readVInt();
        rankConstant = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(windowSize);
        out.writeVInt(rankConstant);
    }

    @Override
    public ActionRequestValidationException validate(ActionRequestValidationException validationException, SearchSourceBuilder source) {
        if (source.size() >= windowSize) {
            validationException = addValidationError(
                "[window size] must be greater than or equal to [size] for [rrf]",
                validationException
            );
        }
        if (rankConstant < 1) {
            validationException = addValidationError("[rank constant] must be greater than [0] for [rrf]", validationException);
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

    public RRFRankContextBuilder windowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public int windowSize() {
        return windowSize;
    }

    public RRFRankContextBuilder rankConstant(int rankConstant) {
        this.rankConstant = rankConstant;
        return this;
    }

    public int rankConstant() {
        return rankConstant;
    }

    @Override
    public RankContextBuilder subShallowCopy() {
        RRFRankContextBuilder rrfRankContextBuilder = new RRFRankContextBuilder();
        rrfRankContextBuilder.windowSize = windowSize;
        rrfRankContextBuilder.rankConstant = rankConstant;
        return rrfRankContextBuilder;
    }

    @Override
    public QueryBuilder searchQuery() {
        BoolQueryBuilder searchQuery = new BoolQueryBuilder();
        for (QueryBuilder queryBuilder : queryBuilders) {
            searchQuery.should(queryBuilder);
        }

        return searchQuery;
    }

    @Override
    public RankShardContext build(SearchExecutionContext searchExecutionContext) throws IOException {
        List<Query> queries = new ArrayList<>();
        for (QueryBuilder queryBuilder : queryBuilders) {
            queries.add(queryBuilder.toQuery(searchExecutionContext));
        }

        return new RRFRankShardContext(queries, size, from, windowSize, rankConstant);
    }

    @Override
    public RankContext build() {
        return new RRFRankContext(queryBuilders, size, from, windowSize, rankConstant);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        RRFRankContextBuilder that = (RRFRankContextBuilder) o;
        return windowSize == that.windowSize && rankConstant == that.rankConstant;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), windowSize, rankConstant);
    }

    @Override
    public String toString() {
        return "RRFRankContextBuilder{"
            + "windowSize="
            + windowSize
            + ", rankConstant="
            + rankConstant
            + ", queryBuilders="
            + queryBuilders
            + ", size="
            + size
            + ", from="
            + from
            + '}';
    }
}

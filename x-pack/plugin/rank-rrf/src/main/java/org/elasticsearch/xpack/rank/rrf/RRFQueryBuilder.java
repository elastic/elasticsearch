/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankQueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RRFQueryBuilder extends AbstractQueryBuilder<RRFQueryBuilder> implements RankQueryBuilder {

    public static final int DEFAULT_WINDOW_SIZE = SearchService.DEFAULT_SIZE;
    public static final int DEFAULT_RANK_CONSTANT = 60;

    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");
    public static final ParseField QUERIES_FIELD = new ParseField("queries");

    private final int windowSize;
    private final int rankConstant;
    private final List<QueryBuilder> queryBuilders;

    static final ConstructingObjectParser<RRFQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(RRFRankPlugin.NAME, args -> {
        int windowSize = args[0] == null ? DEFAULT_WINDOW_SIZE : (int) args[0];
        int rankConstant = args[1] == null ? DEFAULT_RANK_CONSTANT : (int) args[1];
        if (rankConstant < 1) {
            throw new IllegalArgumentException("[rank_constant] must be greater than [0] for [rrf]");
        }
        @SuppressWarnings("unchecked")
        List<QueryBuilder> queryBuilders = (List<QueryBuilder>) args[2];
        return new RRFQueryBuilder(windowSize, rankConstant, queryBuilders);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
        PARSER.declareObjectArray(constructorArg(), (p, c) -> parseInnerQueryBuilder(p), QUERIES_FIELD);
    }

    public static RRFQueryBuilder fromXContent(XContentParser parser) throws IOException {
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.parse(parser, null);
    }

    public RRFQueryBuilder(int windowSize, int rankConstant, List<QueryBuilder> queryBuilders) {
        this.windowSize = windowSize;
        this.rankConstant = rankConstant;
        this.queryBuilders = Collections.unmodifiableList(queryBuilders);
    }

    public RRFQueryBuilder(StreamInput in) throws IOException {
        super(in);
        windowSize = in.readVInt();
        rankConstant = in.readVInt();
        queryBuilders = readQueries(in);
    }

    @Override
    public String getWriteableName() {
        return RRFRankPlugin.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_RRF_QUERY;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(windowSize);
        out.writeVInt(rankConstant);
        writeQueries(out, queryBuilders);
    }

    @Override
    public boolean isFragment() {
        return false;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        List<QueryBuilder> rewritten = new ArrayList<>();
        for (QueryBuilder queryBuilder : queryBuilders) {
            rewritten.add(queryBuilder.rewrite(queryRewriteContext));
        }
        return rewritten.equals(queryBuilders) ? this : new RRFQueryBuilder(windowSize, rankConstant, rewritten);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new IllegalArgumentException("[" + getName() + "] query cannot be used in the current context");
        /*BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
        for (QueryBuilder queryBuilder : queryBuilders) {
            Query query = queryBuilder.toQuery(context);
            booleanQueryBuilder.add(query, BooleanClause.Occur.SHOULD);
        }
        return booleanQueryBuilder.build();*/
    }

    public int getWindowSize() {
        return windowSize;
    }

    public int getRankConstant() {
        return rankConstant;
    }

    @Override
    protected boolean doEquals(RRFQueryBuilder other) {
        return Objects.equals(windowSize, rankConstant);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(windowSize, rankConstant);
    }

    @Override
    public List<QueryBuilder> getChildren() {
        return Collections.unmodifiableList(queryBuilders);
    }

    @Override
    public RankBuilder getRankBuilder() {
        return new RRFRankBuilder(windowSize, rankConstant);
    }
}

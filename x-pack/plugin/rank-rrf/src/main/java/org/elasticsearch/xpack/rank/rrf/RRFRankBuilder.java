/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The builder to support RRF. Adds user-defined parameters for window size and rank constant.
 */
public class RRFRankBuilder extends RankBuilder {

    public static final int DEFAULT_RANK_CONSTANT = 60;

    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    static final ConstructingObjectParser<RRFRankBuilder, Void> PARSER = new ConstructingObjectParser<>(RRFRankPlugin.NAME, args -> {
        int windowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
        int rankConstant = args[1] == null ? DEFAULT_RANK_CONSTANT : (int) args[1];
        if (rankConstant < 1) {
            throw new IllegalArgumentException("[rank_constant] must be greater than [0] for [rrf]");
        }
        return new RRFRankBuilder(windowSize, rankConstant);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
    }

    public static RRFRankBuilder fromXContent(XContentParser parser) throws IOException {
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.parse(parser, null);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }

    private final int rankConstant;

    public RRFRankBuilder(int rankWindowSize, int rankConstant) {
        super(rankWindowSize);
        this.rankConstant = rankConstant;
    }

    public RRFRankBuilder(StreamInput in) throws IOException {
        super(in);
        rankConstant = in.readVInt();
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(rankConstant);
    }

    @Override
    public String getWriteableName() {
        return RRFRankPlugin.NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_8_0;
    }

    public int rankConstant() {
        return rankConstant;
    }

    @Override
    public boolean isCompoundBuilder() {
        return true;
    }

    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return new RRFQueryPhaseRankShardContext(queries, rankWindowSize(), rankConstant);
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return new RRFQueryPhaseRankCoordinatorContext(size, from, rankWindowSize(), rankConstant);
    }

    @Override
    public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return null;
    }

    @Override
    public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return null;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return Objects.equals(rankConstant, ((RRFRankBuilder) other).rankConstant);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(rankConstant);
    }
}

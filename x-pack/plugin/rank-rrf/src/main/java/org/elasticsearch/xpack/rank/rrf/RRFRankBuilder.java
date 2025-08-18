/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The builder to support RRF. Adds user-defined parameters for window size and rank constant.
 *
 * @deprecated RRF support is provided through the retriever framework. Please use {@link RRFRetrieverBuilder instead}
 */
@Deprecated
public class RRFRankBuilder extends RankBuilder {

    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    static final ConstructingObjectParser<RRFRankBuilder, Void> PARSER = new ConstructingObjectParser<>(RRFRankPlugin.NAME, args -> {
        int windowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
        int rankConstant = args[1] == null ? RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT : (int) args[1];
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

        if (rankConstant < 1) {
            throw new IllegalArgumentException("[rank_constant] must be greater or equal to [1] for [rrf]");
        }

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

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc rankDoc, List<String> queryNames) {
        if (rankDoc == null) {
            return baseExplanation;
        }
        if (false == baseExplanation.isMatch()) {
            return baseExplanation;
        }
        final Explanation hitExplanation = baseExplanation.getDetails().length == 0
            ? Explanation.match(baseExplanation.getValue(), baseExplanation.getDescription(), baseExplanation)
            : baseExplanation;

        assert rankDoc instanceof RRFRankDoc : "ScoreDoc is not an instance of RRFRankDoc";
        RRFRankDoc rrfRankDoc = (RRFRankDoc) rankDoc;
        int queries = rrfRankDoc.positions.length;
        assert queryNames.size() == queries;
        Explanation[] details = new Explanation[queries];
        int queryExplainIndex = 0;
        for (int i = 0; i < queries; i++) {
            final String queryName = queryNames.get(i) != null ? "[" + queryNames.get(i) + "]" : "at index [" + i + "]";
            if (rrfRankDoc.positions[i] == RRFRankDoc.NO_RANK) {
                final String description = "rrf score: [0], result not found in query " + queryName;
                details[i] = Explanation.noMatch(description);
            } else {
                final int rank = rrfRankDoc.positions[i] + 1;
                details[i] = Explanation.match(
                    rank,
                    "rrf score: ["
                        + (1f / (rank + rankConstant))
                        + "], "
                        + "for rank ["
                        + (rank)
                        + "] in query "
                        + queryName
                        + " computed as [1 / ("
                        + (rank)
                        + " + "
                        + rankConstant
                        + "]), for matching query with score: ",
                    hitExplanation.getDetails()[queryExplainIndex++]
                );
            }
        }
        return Explanation.match(
            rrfRankDoc.score,
            "rrf score: ["
                + rrfRankDoc.score
                + "] computed for initial ranks "
                + Arrays.toString(Arrays.stream(rrfRankDoc.positions).map(x -> x + 1).toArray())
                + " with rankConstant: ["
                + rankConstant
                + "] as sum of [1 / (rank + rankConstant)] for each query",
            details
        );
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
    public RetrieverBuilder toRetriever(SearchSourceBuilder source, Predicate<NodeFeature> clusterSupportsFeature) {
        int totalQueries = source.subSearches().size() + source.knnSearch().size();
        if (totalQueries < 2) {
            throw new IllegalArgumentException("[rrf] requires at least 2 sub-queries to be defined");
        }
        List<CompoundRetrieverBuilder.RetrieverSource> retrieverSources = new ArrayList<>(totalQueries);
        for (int i = 0; i < source.subSearches().size(); i++) {
            RetrieverBuilder standardRetriever = new StandardRetrieverBuilder(source.subSearches().get(i).getQueryBuilder());
            standardRetriever.retrieverName(source.subSearches().get(i).getQueryBuilder().queryName());
            retrieverSources.add(CompoundRetrieverBuilder.RetrieverSource.from(standardRetriever));
        }
        for (int i = 0; i < source.knnSearch().size(); i++) {
            KnnSearchBuilder knnSearchBuilder = source.knnSearch().get(i);
            RetrieverBuilder knnRetriever = new KnnRetrieverBuilder(
                knnSearchBuilder.getField(),
                knnSearchBuilder.getQueryVector().asFloatVector(),
                knnSearchBuilder.getQueryVectorBuilder(),
                knnSearchBuilder.k(),
                knnSearchBuilder.getNumCands(),
                knnSearchBuilder.getRescoreVectorBuilder(),
                knnSearchBuilder.getSimilarity()
            );
            knnRetriever.retrieverName(knnSearchBuilder.queryName());
            retrieverSources.add(CompoundRetrieverBuilder.RetrieverSource.from(knnRetriever));
        }
        return new RRFRetrieverBuilder(retrieverSources, rankWindowSize(), rankConstant());
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

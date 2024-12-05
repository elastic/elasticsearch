/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * An rrf retriever is used to represent an rrf rank element, but
 * as a tree-like structure. This retriever is a compound retriever
 * meaning it has a set of child retrievers that each return a set of
 * top docs that will then be combined and ranked according to the rrf
 * formula.
 */
public final class RRFRetrieverBuilder extends CompoundRetrieverBuilder<RRFRetrieverBuilder> {

    public static final String NAME = "rrf";
    public static final NodeFeature RRF_RETRIEVER_SUPPORTED = new NodeFeature("rrf_retriever_supported");
    public static final NodeFeature RRF_RETRIEVER_COMPOSITION_SUPPORTED = new NodeFeature("rrf_retriever_composition_supported");

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField RANK_WINDOW_SIZE_FIELD = new ParseField("rank_window_size");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");

    public static final int DEFAULT_RANK_CONSTANT = 60;
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<RRFRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<RetrieverBuilder> childRetrievers = (List<RetrieverBuilder>) args[0];
            List<RetrieverSource> innerRetrievers = childRetrievers.stream().map(r -> new RetrieverSource(r, null)).toList();
            int rankWindowSize = args[1] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
            int rankConstant = args[2] == null ? DEFAULT_RANK_CONSTANT : (int) args[2];
            return new RRFRetrieverBuilder(innerRetrievers, rankWindowSize, rankConstant);
        }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, name, c);
            c.trackRetrieverUsage(retrieverBuilder.getName());
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static RRFRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(RRF_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        if (context.clusterSupportsFeature(RRF_RETRIEVER_COMPOSITION_SUPPORTED) == false) {
            throw new IllegalArgumentException("[rrf] retriever composition feature is not supported by all nodes in the cluster");
        }
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.apply(parser, context);
    }

    private final int rankConstant;

    public RRFRetrieverBuilder(int rankWindowSize, int rankConstant) {
        this(new ArrayList<>(), rankWindowSize, rankConstant);
    }

    RRFRetrieverBuilder(List<RetrieverSource> childRetrievers, int rankWindowSize, int rankConstant) {
        super(childRetrievers, rankWindowSize);
        this.rankConstant = rankConstant;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    protected RRFRetrieverBuilder clone(List<RetrieverSource> newRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        RRFRetrieverBuilder clone = new RRFRetrieverBuilder(newRetrievers, this.rankWindowSize, this.rankConstant);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        return clone;
    }

    @Override
    protected RRFRankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults) {
        // combine the disjointed sets of TopDocs into a single set or RRFRankDocs
        // each RRFRankDoc will have both the position and score for each query where
        // it was within the result set for that query
        // if a doc isn't part of a result set its position will be NO_RANK [0] and
        // its score is [0f]
        int queries = rankResults.size();
        Map<RankDoc.RankKey, RRFRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        int index = 0;
        for (var rrfRankResult : rankResults) {
            int rank = 1;
            for (ScoreDoc scoreDoc : rrfRankResult) {
                final int findex = index;
                final int frank = rank;
                docsToRankResults.compute(new RankDoc.RankKey(scoreDoc.doc, scoreDoc.shardIndex), (key, value) -> {
                    if (value == null) {
                        value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex, queries, rankConstant);
                    }

                    // calculate the current rrf score for this document
                    // later used to sort and covert to a rank
                    value.score += 1.0f / (rankConstant + frank);

                    // record the position for each query
                    // for explain and debugging
                    value.positions[findex] = frank - 1;

                    // record the score for each query
                    // used to later re-rank on the coordinator
                    value.scores[findex] = scoreDoc.score;

                    return value;
                });
                ++rank;
            }
            ++index;
        }

        // sort the results based on rrf score, tiebreaker based on smaller doc id
        RRFRankDoc[] sortedResults = docsToRankResults.values().toArray(RRFRankDoc[]::new);
        Arrays.sort(sortedResults);
        // trim the results if needed, otherwise each shard will always return `rank_window_sieze` results.
        RRFRankDoc[] topResults = new RRFRankDoc[Math.min(rankWindowSize, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
        }
        return topResults;
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public boolean doEquals(Object o) {
        RRFRetrieverBuilder that = (RRFRetrieverBuilder) o;
        return super.doEquals(o) && rankConstant == that.rankConstant;
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), rankConstant);
    }

    @Override
    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        if (innerRetrievers.isEmpty() == false) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());

            for (var entry : innerRetrievers) {
                entry.retriever().toXContent(builder, params);
            }
            builder.endArray();
        }

        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.MatchNoneQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.rank.MultiFieldsInnerRetrieverUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * An rrf retriever is used to represent an rrf rank element, but
 * as a tree-like structure. This retriever is a compound retriever
 * meaning it has a set of child retrievers that each return a set of
 * top docs that will then be combined and ranked according to the rrf
 * formula.
 */
public final class RRFRetrieverBuilder extends CompoundRetrieverBuilder<RRFRetrieverBuilder> {
    public static final NodeFeature MULTI_FIELDS_QUERY_FORMAT_SUPPORT = new NodeFeature("rrf_retriever.multi_fields_query_format_support");

    public static final String NAME = "rrf";

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField RANK_CONSTANT_FIELD = new ParseField("rank_constant");
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField QUERY_FIELD = new ParseField("query");

    public static final int DEFAULT_RANK_CONSTANT = 60;
    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<RRFRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<RetrieverBuilder> childRetrievers = (List<RetrieverBuilder>) args[0];
            List<String> fields = (List<String>) args[1];
            String query = (String) args[2];
            int rankWindowSize = args[3] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[3];
            int rankConstant = args[4] == null ? DEFAULT_RANK_CONSTANT : (int) args[4];

            List<RetrieverSource> innerRetrievers = childRetrievers != null
                ? childRetrievers.stream().map(RetrieverSource::from).toList()
                : List.of();
            return new RRFRetrieverBuilder(innerRetrievers, fields, query, rankWindowSize, rankConstant);
        }
    );

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> {
            p.nextToken();
            String name = p.currentName();
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, name, c);
            c.trackRetrieverUsage(retrieverBuilder.getName());
            p.nextToken();
            return retrieverBuilder;
        }, RETRIEVERS_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), FIELDS_FIELD);
        PARSER.declareString(optionalConstructorArg(), QUERY_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_CONSTANT_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    public static RRFRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (RRFRankPlugin.RANK_RRF_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("Reciprocal Rank Fusion (RRF)");
        }
        return PARSER.apply(parser, context);
    }

    private final List<String> fields;
    private final String query;
    private final int rankConstant;

    public RRFRetrieverBuilder(List<RetrieverSource> childRetrievers, int rankWindowSize, int rankConstant) {
        this(childRetrievers, null, null, rankWindowSize, rankConstant);
    }

    public RRFRetrieverBuilder(
        List<RetrieverSource> childRetrievers,
        List<String> fields,
        String query,
        int rankWindowSize,
        int rankConstant
    ) {
        // Use a mutable list for childRetrievers so that we can use addChild
        super(childRetrievers == null ? new ArrayList<>() : new ArrayList<>(childRetrievers), rankWindowSize);
        this.fields = fields == null ? null : List.copyOf(fields);
        this.query = query;
        this.rankConstant = rankConstant;
    }

    public int rankConstant() {
        return rankConstant;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        validationException = super.validate(source, validationException, isScroll, allowPartialSearchResults);
        return MultiFieldsInnerRetrieverUtils.validateParams(
            innerRetrievers,
            fields,
            query,
            getName(),
            RETRIEVERS_FIELD.getPreferredName(),
            FIELDS_FIELD.getPreferredName(),
            QUERY_FIELD.getPreferredName(),
            validationException
        );
    }

    @Override
    protected RRFRetrieverBuilder clone(List<RetrieverSource> newRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        RRFRetrieverBuilder clone = new RRFRetrieverBuilder(newRetrievers, this.fields, this.query, this.rankWindowSize, this.rankConstant);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        clone.retrieverName = retrieverName;
        return clone;
    }

    @Override
    protected RRFRankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean explain) {
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
                        if (explain) {
                            value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex, queries, rankConstant);
                        } else {
                            value = new RRFRankDoc(scoreDoc.doc, scoreDoc.shardIndex);
                        }
                    }

                    // calculate the current rrf score for this document
                    // later used to sort and covert to a rank
                    value.score += 1.0f / (rankConstant + frank);

                    if (explain && value.positions != null && value.scores != null) {
                        // record the position for each query
                        // for explain and debugging
                        value.positions[findex] = frank - 1;

                        // record the score for each query
                        // used to later re-rank on the coordinator
                        value.scores[findex] = scoreDoc.score;
                    }

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

    @Override
    protected RetrieverBuilder doRewrite(QueryRewriteContext ctx) {
        RetrieverBuilder rewritten = this;

        ResolvedIndices resolvedIndices = ctx.getResolvedIndices();
        if (resolvedIndices != null && query != null) {
            // TODO: Refactor duplicate code
            // Using the multi-fields query format
            var localIndicesMetadata = resolvedIndices.getConcreteLocalIndicesMetadata();
            if (localIndicesMetadata.size() > 1) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] cannot specify [" + QUERY_FIELD.getPreferredName() + "] when querying multiple indices"
                );
            } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] cannot specify [" + QUERY_FIELD.getPreferredName() + "] when querying remote indices"
                );
            }

            List<RetrieverSource> fieldsInnerRetrievers = MultiFieldsInnerRetrieverUtils.generateInnerRetrievers(
                fields,
                query,
                localIndicesMetadata.values(),
                r -> {
                    List<RetrieverSource> retrievers = r.stream()
                        .map(MultiFieldsInnerRetrieverUtils.WeightedRetrieverSource::retrieverSource)
                        .toList();
                    return new RRFRetrieverBuilder(retrievers, rankWindowSize, rankConstant);
                },
                w -> {
                    if (w != 1.0f) {
                        throw new IllegalArgumentException(
                            "[" + NAME + "] does not support per-field weights in [" + FIELDS_FIELD.getPreferredName() + "]"
                        );
                    }
                }
            ).stream().map(RetrieverSource::from).toList();

            if (fieldsInnerRetrievers.isEmpty() == false) {
                // TODO: This is a incomplete solution as it does not address other incomplete copy issues
                // (such as dropping the retriever name and min score)
                rewritten = new RRFRetrieverBuilder(fieldsInnerRetrievers, rankWindowSize, rankConstant);
                rewritten.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
            } else {
                // Inner retriever list can be empty when using an index wildcard pattern that doesn't match any indices
                rewritten = new StandardRetrieverBuilder(new MatchNoneQueryBuilder());
            }
        }

        return rewritten;
    }

    // ---- FOR TESTING XCONTENT PARSING ----

    @Override
    public boolean doEquals(Object o) {
        RRFRetrieverBuilder that = (RRFRetrieverBuilder) o;
        return super.doEquals(o)
            && Objects.equals(fields, that.fields)
            && Objects.equals(query, that.query)
            && rankConstant == that.rankConstant;
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), fields, query, rankConstant);
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

        if (fields != null) {
            builder.startArray(FIELDS_FIELD.getPreferredName());
            for (String field : fields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName(), query);
        }

        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        builder.field(RANK_CONSTANT_FIELD.getPreferredName(), rankConstant);
    }
}

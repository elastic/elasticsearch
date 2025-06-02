/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
import org.elasticsearch.xpack.rank.rrf.RRFRankPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.rank.RankRRFFeatures.LINEAR_RETRIEVER_SUPPORTED;
import static org.elasticsearch.xpack.rank.linear.LinearRetrieverComponent.DEFAULT_WEIGHT;

/**
 * The {@code LinearRetrieverBuilder} supports the combination of different retrievers through a weighted linear combination.
 * For example, assume that we have retrievers r1 and r2, the final score of the {@code LinearRetrieverBuilder} is defined as
 * {@code score(r)=w1*score(r1) + w2*score(r2)}.
 * Each sub-retriever score can be normalized before being considered for the weighted linear sum, by setting the appropriate
 * normalizer parameter.
 *
 */
public final class LinearRetrieverBuilder extends CompoundRetrieverBuilder<LinearRetrieverBuilder> {

    public static final String NAME = "linear";

    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    public static final float DEFAULT_SCORE = 0f;

    private final float[] weights;
    private final ScoreNormalizer[] normalizers;
    private final Float minScore;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<LinearRetrieverComponent> retrieverComponents = (List<LinearRetrieverComponent>) args[0];
            int rankWindowSize = args[1] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
            Float minScore = (Float) args[2];
            List<RetrieverSource> innerRetrievers = new ArrayList<>();
            float[] weights = new float[retrieverComponents.size()];
            ScoreNormalizer[] normalizers = new ScoreNormalizer[retrieverComponents.size()];
            int index = 0;
            for (LinearRetrieverComponent component : retrieverComponents) {
                innerRetrievers.add(new RetrieverSource(component.retriever, null));
                weights[index] = component.weight;
                normalizers[index] = component.normalizer;
                index++;
            }
            return new LinearRetrieverBuilder(innerRetrievers, rankWindowSize, weights, normalizers, minScore);
        }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), LinearRetrieverComponent::fromXContent, RETRIEVERS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private static float[] getDefaultWeight(int size) {
        float[] weights = new float[size];
        Arrays.fill(weights, DEFAULT_WEIGHT);
        return weights;
    }

    private static ScoreNormalizer[] getDefaultNormalizers(int size) {
        ScoreNormalizer[] normalizers = new ScoreNormalizer[size];
        Arrays.fill(normalizers, IdentityScoreNormalizer.INSTANCE);
        return normalizers;
    }

    public static LinearRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(LINEAR_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        if (RRFRankPlugin.LINEAR_RETRIEVER_FEATURE.check(XPackPlugin.getSharedLicenseState()) == false) {
            throw LicenseUtils.newComplianceException("linear retriever");
        }
        return PARSER.apply(parser, context);
    }

    LinearRetrieverBuilder(List<RetrieverSource> innerRetrievers, int rankWindowSize) {
        this(
            innerRetrievers,
            rankWindowSize,
            getDefaultWeight(innerRetrievers.size()),
            getDefaultNormalizers(innerRetrievers.size()),
            null
        );
    }

    public LinearRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        int rankWindowSize,
        float[] weights,
        ScoreNormalizer[] normalizers,
        Float minScore
    ) {
        super(innerRetrievers, rankWindowSize);
        if (weights.length != innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of weights must match the number of inner retrievers");
        }
        if (normalizers.length != innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of normalizers must match the number of inner retrievers");
        }
        if (minScore != null && minScore < 0.0f) {
            throw new IllegalArgumentException(
                "[" + MIN_SCORE_FIELD.getPreferredName() + "] must be greater than or equal to 0, was: " + minScore
            );
        }
        this.weights = weights;
        this.normalizers = normalizers;
        this.minScore = minScore;
    }

    @Override
    protected LinearRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        LinearRetrieverBuilder clone = new LinearRetrieverBuilder(newChildRetrievers, rankWindowSize, weights, normalizers, minScore);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        clone.retrieverName = retrieverName;
        return clone;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        sourceBuilder.trackScores(true);
        return sourceBuilder;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean isExplain) {
        Map<RankDoc.RankKey, LinearRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        final String[] normalizerNames = Arrays.stream(normalizers).map(ScoreNormalizer::getName).toArray(String[]::new);
        for (int result = 0; result < rankResults.size(); result++) {
            final ScoreNormalizer normalizer = normalizers[result] == null ? IdentityScoreNormalizer.INSTANCE : normalizers[result];
            ScoreDoc[] originalScoreDocs = rankResults.get(result);
            ScoreDoc[] normalizedScoreDocs = normalizer.normalizeScores(originalScoreDocs);
            for (int scoreDocIndex = 0; scoreDocIndex < normalizedScoreDocs.length; scoreDocIndex++) {
                LinearRankDoc rankDoc = docsToRankResults.computeIfAbsent(
                    new RankDoc.RankKey(originalScoreDocs[scoreDocIndex].doc, originalScoreDocs[scoreDocIndex].shardIndex),
                    key -> {
                        if (isExplain) {
                            LinearRankDoc doc = new LinearRankDoc(key.doc(), 0f, key.shardIndex(), weights, normalizerNames);
                            doc.normalizedScores = new float[rankResults.size()];
                            return doc;
                        } else {
                            return new LinearRankDoc(key.doc(), 0f, key.shardIndex());
                        }
                    }
                );
                if (isExplain) {
                    rankDoc.normalizedScores[result] = normalizedScoreDocs[scoreDocIndex].score;
                }
                // if we do not have scores associated with this result set, just ignore its contribution to the final
                // score computation by setting its score to 0.
                final float docScore = false == Float.isNaN(normalizedScoreDocs[scoreDocIndex].score)
                    ? normalizedScoreDocs[scoreDocIndex].score
                    : DEFAULT_SCORE;
                final float weight = Float.isNaN(weights[result]) ? DEFAULT_WEIGHT : weights[result];
                rankDoc.score += weight * docScore;
            }
        }
        // sort the results based on the final score, tiebreaker based on smaller doc id
        LinearRankDoc[] sortedResults = docsToRankResults.values().toArray(LinearRankDoc[]::new);
        Arrays.sort(sortedResults);

        final LinearRankDoc[] resultsToConsider;
        if (minScore != null) { // Check if minScore was actually set
            List<LinearRankDoc> filteredList = new ArrayList<>(sortedResults.length);
            for (LinearRankDoc doc : sortedResults) {
                if (doc.score >= minScore) {
                    filteredList.add(doc);
                }
            }
            resultsToConsider = filteredList.toArray(LinearRankDoc[]::new);
        } else {
            resultsToConsider = sortedResults; // No filtering if minScore is null
        }

        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        LinearRankDoc[] topResults = new LinearRankDoc[Math.min(rankWindowSize, sortedResults.length)];
        for (int rank = 0; rank < topResults.length; ++rank) {
            topResults[rank] = sortedResults[rank];
            topResults[rank].rank = rank + 1;
        }
        return topResults;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public void doToXContent(XContentBuilder builder, Params params) throws IOException {
        int index = 0;
        if (innerRetrievers.isEmpty() == false) {
            builder.startArray(RETRIEVERS_FIELD.getPreferredName());
            for (var entry : innerRetrievers) {
                builder.startObject();
                builder.field(LinearRetrieverComponent.RETRIEVER_FIELD.getPreferredName(), entry.retriever());
                builder.field(LinearRetrieverComponent.WEIGHT_FIELD.getPreferredName(), weights[index]);
                builder.field(LinearRetrieverComponent.NORMALIZER_FIELD.getPreferredName(), normalizers[index].getName());
                builder.endObject();
                index++;
            }
            builder.endArray();
        }
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
    }
}

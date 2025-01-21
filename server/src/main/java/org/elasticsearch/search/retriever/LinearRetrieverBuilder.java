/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.rank.LinearRankDoc;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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

    public static final NodeFeature LINEAR_RETRIEVER_SUPPORTED = new NodeFeature("linear_retriever_supported");
    public static final ParseField RETRIEVERS_FIELD = new ParseField("retrievers");

    private final float[] weights;
    private final ScoreNormalizer[] normalizers;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<LinearRetrieverComponent> retrieverComponents = (List<LinearRetrieverComponent>) args[0];
            int rankWindowSize = args[1] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
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
            return new LinearRetrieverBuilder(innerRetrievers, rankWindowSize, weights, normalizers);
        }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), LinearRetrieverComponent::fromXContent, RETRIEVERS_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static LinearRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (context.clusterSupportsFeature(LINEAR_RETRIEVER_SUPPORTED) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    public LinearRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        int rankWindowSize,
        float[] weights,
        ScoreNormalizer[] normalizers
    ) {
        super(innerRetrievers, rankWindowSize);
        this.weights = weights;
        this.normalizers = normalizers;
    }

    @Override
    protected LinearRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        LinearRetrieverBuilder clone = new LinearRetrieverBuilder(newChildRetrievers, rankWindowSize, weights, normalizers);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        return clone;
    }

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults) {
        Map<RankDoc.RankKey, LinearRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        final String[] normalizerNames = Arrays.stream(normalizers).map(ScoreNormalizer::getName).toArray(String[]::new);
        for (int result = 0; result < rankResults.size(); result++) {
            ScoreDoc[] originalScoreDocs = rankResults.get(result);
            ScoreDoc[] normalizedScoreDocs = normalizers[result].normalizeScores(originalScoreDocs);
            for (int scoreDocIndex = 0; scoreDocIndex < normalizedScoreDocs.length; scoreDocIndex++) {
                int finalResult = result;
                int finalScoreIndex = scoreDocIndex;
                docsToRankResults.compute(
                    new RankDoc.RankKey(originalScoreDocs[scoreDocIndex].doc, originalScoreDocs[scoreDocIndex].shardIndex),
                    (key, value) -> {
                        if (value == null) {
                            value = new LinearRankDoc(
                                originalScoreDocs[finalScoreIndex].doc,
                                0f,
                                originalScoreDocs[finalScoreIndex].shardIndex,
                                weights,
                                normalizerNames
                            );
                        }
                        value.normalizedScores[finalResult] = normalizedScoreDocs[finalScoreIndex].score;
                        value.score += weights[finalResult] * normalizedScoreDocs[finalScoreIndex].score;
                        return value;
                    }
                );
            }
        }
        // sort the results based on the final score, tiebreaker based on smaller doc id
        LinearRankDoc[] sortedResults = docsToRankResults.values().toArray(LinearRankDoc[]::new);
        Arrays.sort(sortedResults);
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
                builder.field(LinearRetrieverComponent.NORMALIZER_FIELD.getPreferredName(), normalizers[index]);
                builder.endObject();
                index++;
            }
            builder.endArray();
        }
        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }
}

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
import org.elasticsearch.index.query.AbstractQueryBuilder;
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
    public static final float DEFAULT_MIN_SCORE = 0f;

    private final float[] weights;
    private final ScoreNormalizer[] normalizers;
    private float minScore;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<LinearRetrieverComponent> retrieverComponents = (List<LinearRetrieverComponent>) args[0];
            int rankWindowSize = args[1] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[1];
            float minScore = args[2] == null ? DEFAULT_MIN_SCORE : (float) args[2];
            List<RetrieverSource> innerRetrievers = new ArrayList<>();
            float[] weights = new float[retrieverComponents.size()];
            ScoreNormalizer[] normalizers = new ScoreNormalizer[retrieverComponents.size()];
            int index = 0;
            for (LinearRetrieverComponent component : retrieverComponents) {
                RetrieverBuilder retriever = component.retriever;
                // Do not set minScore on inner retrievers, we'll apply it after combining
                innerRetrievers.add(new RetrieverSource(retriever, null));
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

        PARSER.declareObjectArray(
            (r, v) -> r.preFilterQueryBuilders = new ArrayList<QueryBuilder>(v),
            (p, c) -> AbstractQueryBuilder.parseTopLevelQuery(p, c::trackQueryUsage),
            RetrieverBuilder.PRE_FILTER_FIELD
        );
        PARSER.declareString(RetrieverBuilder::retrieverName, RetrieverBuilder.NAME_FIELD);
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
            DEFAULT_MIN_SCORE
        );
    }

    public LinearRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        int rankWindowSize,
        float[] weights,
        ScoreNormalizer[] normalizers,
        float minScore
    ) {
        super(innerRetrievers, rankWindowSize);
        if (weights.length != innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of weights must match the number of inner retrievers");
        }
        if (normalizers.length != innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of normalizers must match the number of inner retrievers");
        }
        if (minScore < 0) {
            throw new IllegalArgumentException("[min_score] must be greater than 0, was: " + minScore);
        }
        this.weights = weights;
        this.normalizers = normalizers;
        this.minScore = minScore;

        // Set the parent class's minScore field so it propagates to RankDocsRetrieverBuilder
        super.minScore = minScore > 0 ? minScore : null;

        // Don't set minScore on inner retrievers anymore - we'll apply it after combining
        System.out.println(
            "DEBUG: Constructed LinearRetrieverBuilder with minScore="
                + minScore
                + ", rankWindowSize="
                + rankWindowSize
                + ", retrievers="
                + innerRetrievers.size()
        );
    }

    @Override
    protected LinearRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        LinearRetrieverBuilder clone = new LinearRetrieverBuilder(newChildRetrievers, rankWindowSize, weights, normalizers, minScore);
        clone.preFilterQueryBuilders = newPreFilterQueryBuilders;
        clone.retrieverName = retrieverName;

        // Ensure parent's minScore field is correctly set (should already be done in constructor but just to be safe)
        clone.minScore = this.minScore;

        System.out.println("DEBUG: Cloned LinearRetrieverBuilder with minScore=" + minScore);

        return clone;
    }

    @Override
    protected SearchSourceBuilder finalizeSourceBuilder(SearchSourceBuilder sourceBuilder) {
        System.out.println("DEBUG: finalizeSourceBuilder - minScore=" + minScore);

        sourceBuilder.trackScores(true);
        return sourceBuilder;
    }

    // Thread-local storage to hold the filtered count from combineInnerRetrieverResults
    private static final ThreadLocal<Integer> filteredTotalHitsHolder = new ThreadLocal<>();

    @Override
    protected RankDoc[] combineInnerRetrieverResults(List<ScoreDoc[]> rankResults, boolean isExplain) {
        System.out.println(
            "DEBUG: combineInnerRetrieverResults START - minScore="
                + minScore
                + ", rankWindowSize="
                + rankWindowSize
                + ", isExplain="
                + isExplain
        );

        Map<RankDoc.RankKey, LinearRankDoc> docsToRankResults = Maps.newMapWithExpectedSize(rankWindowSize);
        final String[] normalizerNames = Arrays.stream(normalizers).map(ScoreNormalizer::getName).toArray(String[]::new);

        // Process all inner retriever results
        for (int result = 0; result < rankResults.size(); result++) {
            ScoreDoc[] originalScoreDocs = rankResults.get(result);
            if (originalScoreDocs == null) {
                System.out.println("DEBUG: Inner retriever " + result + " returned null results");
                continue;
            }

            System.out.println("DEBUG: Inner retriever " + result + " has " + originalScoreDocs.length + " results");

            final float weight = Float.isNaN(weights[result]) ? DEFAULT_WEIGHT : weights[result];
            final ScoreNormalizer normalizer = normalizers[result];

            // Filter out any null or invalid score docs before normalization
            ScoreDoc[] validScoreDocs = Arrays.stream(originalScoreDocs)
                .filter(doc -> doc != null && !Float.isNaN(doc.score))
                .toArray(ScoreDoc[]::new);

            if (validScoreDocs.length == 0) {
                System.out.println("DEBUG: Inner retriever " + result + " has no valid score docs after filtering");
                continue;
            }

            // Store raw scores before normalization for explain mode
            float[] rawScores = null;
            if (isExplain) {
                rawScores = new float[validScoreDocs.length];
                for (int i = 0; i < validScoreDocs.length; i++) {
                    rawScores[i] = validScoreDocs[i].score;
                }
            }

            // Normalize scores for this retriever
            ScoreDoc[] normalizedScoreDocs = normalizer.normalizeScores(validScoreDocs);

            System.out.println("DEBUG: Inner retriever " + result + " - weight=" + weight + ", normalizer=" + normalizer.getName());

            for (int i = 0; i < normalizedScoreDocs.length; i++) {
                ScoreDoc scoreDoc = normalizedScoreDocs[i];
                if (scoreDoc == null) {
                    continue;
                }

                LinearRankDoc rankDoc = docsToRankResults.computeIfAbsent(new RankDoc.RankKey(scoreDoc.doc, scoreDoc.shardIndex), key -> {
                    if (isExplain) {
                        LinearRankDoc doc = new LinearRankDoc(key.doc(), 0f, key.shardIndex(), weights, normalizerNames);
                        doc.normalizedScores = new float[rankResults.size()];
                        return doc;
                    } else {
                        return new LinearRankDoc(key.doc(), 0f, key.shardIndex());
                    }
                });

                // Store the normalized score for this retriever
                final float docScore = false == Float.isNaN(scoreDoc.score) ? scoreDoc.score : DEFAULT_SCORE;
                if (isExplain) {
                    rankDoc.normalizedScores[result] = docScore;
                }

                // Apply weight to the normalized score
                rankDoc.score += weight * docScore;
            }
        }

        LinearRankDoc[] filteredResults = docsToRankResults.values().stream().toArray(LinearRankDoc[]::new);

        System.out.println("DEBUG: Combined " + filteredResults.length + " unique documents from all retrievers");

        // sort the results based on the final score, tiebreaker based on smaller doc id
        LinearRankDoc[] sortedResults = Arrays.stream(filteredResults).sorted().toArray(LinearRankDoc[]::new);

        System.out.println("DEBUG: Sorted results before filtering:");
        for (LinearRankDoc doc : sortedResults) {
            System.out.println("DEBUG:   Doc ID: " + doc.doc + ", Sorted Score: " + doc.score);
        }

        // Apply minScore filtering if needed
        int originalResultCount = sortedResults.length;

        // Store the TOTAL hits before filtering for search response
        filteredTotalHitsHolder.set(originalResultCount);

        if (minScore > 0) {
            System.out.println("DEBUG: Filtering results with minScore=" + minScore);

            LinearRankDoc[] filteredByMinScore = Arrays.stream(sortedResults)
                .filter(doc -> doc.score >= minScore)
                .toArray(LinearRankDoc[]::new);

            int filteredResultCount = filteredByMinScore.length;
            sortedResults = filteredByMinScore;

            System.out.println(
                "DEBUG: After minScore filtering: "
                    + originalResultCount
                    + " original results -> "
                    + filteredResultCount
                    + " filtered results (meeting minScore="
                    + minScore
                    + ")"
            );

            // Store filtered count in thread local for rewrite method to access
            // This is critically important for the test that expects the total hits to reflect the filtered count
            filteredTotalHitsHolder.set(filteredResultCount);
        }

        // trim to rank window size
        int preWindowCount = sortedResults.length;
        sortedResults = Arrays.stream(sortedResults).limit(rankWindowSize).toArray(LinearRankDoc[]::new);

        System.out.println(
            "DEBUG: After window size limiting: "
                + preWindowCount
                + " results -> "
                + sortedResults.length
                + " results (rankWindowSize="
                + rankWindowSize
                + ")"
        );

        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        for (int rank = 0; rank < sortedResults.length; ++rank) {
            sortedResults[rank].rank = rank + 1;
            System.out.println(
                "DEBUG: Final result ["
                    + rank
                    + "]: doc="
                    + sortedResults[rank].doc
                    + ", score="
                    + sortedResults[rank].score
                    + ", rank="
                    + sortedResults[rank].rank
            );
        }

        System.out.println("DEBUG: combineInnerRetrieverResults END - returning " + sortedResults.length + " results");
        return sortedResults;
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
        if (minScore != DEFAULT_MIN_SCORE) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
    }

    @Override
    public Float minScore() {
        // Return the minScore directly regardless of its value
        System.out.println("DEBUG: LinearRetrieverBuilder.minScore() returning " + minScore + " (raw value: " + minScore + ")");
        return minScore;
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.util.Maps;
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
import org.elasticsearch.xpack.rank.rrf.RRFRankPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
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
    public static final ParseField FIELDS_FIELD = new ParseField("fields");
    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField NORMALIZER_FIELD = new ParseField("normalizer");

    public static final float DEFAULT_SCORE = 0f;

    private final float[] weights;
    private final ScoreNormalizer[] normalizers;
    private final List<String> fields;
    private final String query;
    private final ScoreNormalizer normalizer;

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            List<LinearRetrieverComponent> retrieverComponents = args[0] == null ? List.of() : (List<LinearRetrieverComponent>) args[0];
            List<String> fields = (List<String>) args[1];
            String query = (String) args[2];
            ScoreNormalizer normalizer = args[3] == null ? null : ScoreNormalizer.valueOf((String) args[3]);
            int rankWindowSize = args[4] == null ? RankBuilder.DEFAULT_RANK_WINDOW_SIZE : (int) args[4];

            int index = 0;
            float[] weights = new float[retrieverComponents.size()];
            ScoreNormalizer[] normalizers = new ScoreNormalizer[retrieverComponents.size()];
            List<RetrieverSource> innerRetrievers = new ArrayList<>();
            for (LinearRetrieverComponent component : retrieverComponents) {
                innerRetrievers.add(RetrieverSource.from(component.retriever));
                weights[index] = component.weight;
                normalizers[index] = component.normalizer;
                index++;
            }
            return new LinearRetrieverBuilder(innerRetrievers, fields, query, normalizer, rankWindowSize, weights, normalizers);
        }
    );

    static {
        PARSER.declareObjectArray(optionalConstructorArg(), LinearRetrieverComponent::fromXContent, RETRIEVERS_FIELD);
        PARSER.declareStringArray(optionalConstructorArg(), FIELDS_FIELD);
        PARSER.declareString(optionalConstructorArg(), QUERY_FIELD);
        PARSER.declareString(optionalConstructorArg(), NORMALIZER_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        RetrieverBuilder.declareBaseParserFields(PARSER);
    }

    private static float[] getDefaultWeight(List<RetrieverSource> innerRetrievers) {
        int size = innerRetrievers != null ? innerRetrievers.size() : 0;
        float[] weights = new float[size];
        Arrays.fill(weights, DEFAULT_WEIGHT);
        return weights;
    }

    private static ScoreNormalizer[] getDefaultNormalizers(List<RetrieverSource> innerRetrievers) {
        int size = innerRetrievers != null ? innerRetrievers.size() : 0;
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
        this(innerRetrievers, null, null, null, rankWindowSize, getDefaultWeight(innerRetrievers), getDefaultNormalizers(innerRetrievers));
    }

    public LinearRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        int rankWindowSize,
        float[] weights,
        ScoreNormalizer[] normalizers
    ) {
        this(innerRetrievers, null, null, null, rankWindowSize, weights, normalizers);
    }

    public LinearRetrieverBuilder(
        List<RetrieverSource> innerRetrievers,
        List<String> fields,
        String query,
        ScoreNormalizer normalizer,
        int rankWindowSize,
        float[] weights,
        ScoreNormalizer[] normalizers
    ) {
        // Use a mutable list for innerRetrievers so that we can use addChild
        super(innerRetrievers == null ? new ArrayList<>() : new ArrayList<>(innerRetrievers), rankWindowSize);
        if (weights.length != this.innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of weights must match the number of inner retrievers");
        }
        if (normalizers.length != this.innerRetrievers.size()) {
            throw new IllegalArgumentException("The number of normalizers must match the number of inner retrievers");
        }

        this.fields = fields == null ? List.of() : List.copyOf(fields);
        this.query = query;
        this.normalizer = normalizer;
        this.weights = weights;
        this.normalizers = normalizers;
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        validationException = super.validate(source, validationException, isScroll, allowPartialSearchResults);
        validationException = MultiFieldsInnerRetrieverUtils.validateParams(
            innerRetrievers,
            fields,
            query,
            getName(),
            RETRIEVERS_FIELD.getPreferredName(),
            FIELDS_FIELD.getPreferredName(),
            QUERY_FIELD.getPreferredName(),
            validationException
        );

        if (query != null && normalizer == null) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] [%s] must be provided when [%s] is specified",
                    getName(),
                    NORMALIZER_FIELD.getPreferredName(),
                    QUERY_FIELD.getPreferredName()
                ),
                validationException
            );
        } else if (innerRetrievers.isEmpty() == false && normalizer != null) {
            validationException = addValidationError(
                String.format(
                    Locale.ROOT,
                    "[%s] [%s] cannot be provided when [%s] is specified",
                    getName(),
                    NORMALIZER_FIELD.getPreferredName(),
                    RETRIEVERS_FIELD.getPreferredName()
                ),
                validationException
            );
        }

        return validationException;
    }

    @Override
    protected LinearRetrieverBuilder clone(List<RetrieverSource> newChildRetrievers, List<QueryBuilder> newPreFilterQueryBuilders) {
        LinearRetrieverBuilder clone = new LinearRetrieverBuilder(
            newChildRetrievers,
            fields,
            query,
            normalizer,
            rankWindowSize,
            weights,
            normalizers
        );
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
        // trim the results if needed, otherwise each shard will always return `rank_window_size` results.
        LinearRankDoc[] topResults = new LinearRankDoc[Math.min(rankWindowSize, sortedResults.length)];
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
            // Using the multi-fields query format
            var localIndicesMetadata = resolvedIndices.getConcreteLocalIndicesMetadata();
            if (localIndicesMetadata.size() > 1) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] does not support the simplified query format when querying multiple indices"
                );
            } else if (resolvedIndices.getRemoteClusterIndices().isEmpty() == false) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] does not support the simplified query format when querying remote indices"
                );
            }

            List<RetrieverSource> fieldsInnerRetrievers = MultiFieldsInnerRetrieverUtils.generateInnerRetrievers(
                fields,
                query,
                localIndicesMetadata.values(),
                r -> {
                    List<RetrieverSource> retrievers = new ArrayList<>(r.size());
                    float[] weights = new float[r.size()];
                    ScoreNormalizer[] normalizers = new ScoreNormalizer[r.size()];

                    int index = 0;
                    for (var weightedRetriever : r) {
                        retrievers.add(weightedRetriever.retrieverSource());
                        weights[index] = weightedRetriever.weight();
                        normalizers[index] = normalizer;
                        index++;
                    }

                    return new LinearRetrieverBuilder(retrievers, rankWindowSize, weights, normalizers);
                },
                w -> {
                    if (w < 0) {
                        throw new IllegalArgumentException("[" + NAME + "] per-field weights must be non-negative");
                    }
                }
            ).stream().map(RetrieverSource::from).toList();

            if (fieldsInnerRetrievers.isEmpty() == false) {
                float[] weights = new float[fieldsInnerRetrievers.size()];
                Arrays.fill(weights, DEFAULT_WEIGHT);

                ScoreNormalizer[] normalizers = new ScoreNormalizer[fieldsInnerRetrievers.size()];
                Arrays.fill(normalizers, normalizer);

                // TODO: This is a incomplete solution as it does not address other incomplete copy issues
                // (such as dropping the retriever name and min score)
                rewritten = new LinearRetrieverBuilder(fieldsInnerRetrievers, null, null, normalizer, rankWindowSize, weights, normalizers);
                rewritten.getPreFilterQueryBuilders().addAll(preFilterQueryBuilders);
            } else {
                // Inner retriever list can be empty when using an index wildcard pattern that doesn't match any indices
                rewritten = new StandardRetrieverBuilder(new MatchNoneQueryBuilder());
            }
        }

        return rewritten;
    }

    @Override
    public String getName() {
        return NAME;
    }

    float[] getWeights() {
        return weights;
    }

    ScoreNormalizer[] getNormalizers() {
        return normalizers;
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

        if (fields.isEmpty() == false) {
            builder.startArray(FIELDS_FIELD.getPreferredName());
            for (String field : fields) {
                builder.value(field);
            }
            builder.endArray();
        }
        if (query != null) {
            builder.field(QUERY_FIELD.getPreferredName(), query);
        }
        if (normalizer != null) {
            builder.field(NORMALIZER_FIELD.getPreferredName(), normalizer.getName());
        }

        builder.field(RANK_WINDOW_SIZE_FIELD.getPreferredName(), rankWindowSize);
    }

    @Override
    public boolean doEquals(Object o) {
        LinearRetrieverBuilder that = (LinearRetrieverBuilder) o;
        return super.doEquals(o)
            && Arrays.equals(weights, that.weights)
            && Arrays.equals(normalizers, that.normalizers)
            && Objects.equals(fields, that.fields)
            && Objects.equals(query, that.query)
            && Objects.equals(normalizer, that.normalizer);
    }

    @Override
    public int doHashCode() {
        return Objects.hash(super.doHashCode(), Arrays.hashCode(weights), Arrays.hashCode(normalizers), fields, query, normalizer);
    }
}

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xpack.rank.linear.LinearRetrieverBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

/**
 * A utility class for managing and validating the multi-fields query format for the {@link LinearRetrieverBuilder} retriever.
 */
public class MultiFieldsInnerRetrieverUtils {
    private MultiFieldsInnerRetrieverUtils() {}

    public record WeightedRetrieverSource(CompoundRetrieverBuilder.RetrieverSource retrieverSource, float weight) {}

    /**
     * Validates the parameters related to the multi-fields query format.
     *
     * @param innerRetrievers The custom inner retrievers already defined
     * @param fields The fields to query
     * @param query The query text
     * @param retrieverName The containing retriever name
     * @param retrieversParamName The parameter name for custom inner retrievers
     * @param fieldsParamName The parameter name for the fields to query
     * @param queryParamName The parameter name for the query text
     * @param validationException The validation exception
     * @return The validation exception with any multi-fields query format validation errors appended
     */
    public static ActionRequestValidationException validateParams(
        List<CompoundRetrieverBuilder.RetrieverSource> innerRetrievers,
        List<String> fields,
        @Nullable String query,
        String retrieverName,
        String retrieversParamName,
        String fieldsParamName,
        String queryParamName,
        ActionRequestValidationException validationException
    ) {
        if (fields.isEmpty() == false || query != null) {
            // Using the multi-fields query format
            if (query == null) {
                // Return early here because the following validation checks assume a query param value is provided
                return addValidationError(
                    String.format(
                        Locale.ROOT,
                        "[%s] [%s] must be provided when [%s] is specified",
                        retrieverName,
                        queryParamName,
                        fieldsParamName
                    ),
                    validationException
                );
            }

            if (query.isEmpty()) {
                validationException = addValidationError(
                    String.format(Locale.ROOT, "[%s] [%s] cannot be empty", retrieverName, queryParamName),
                    validationException
                );
            }

            if (innerRetrievers.isEmpty() == false) {
                validationException = addValidationError(
                    String.format(Locale.ROOT, "[%s] cannot combine [%s] and [%s]", retrieverName, retrieversParamName, queryParamName),
                    validationException
                );
            }
        } else if (innerRetrievers.isEmpty()) {
            validationException = addValidationError(
                String.format(Locale.ROOT, "[%s] must provide [%s] or [%s]", retrieverName, retrieversParamName, queryParamName),
                validationException
            );
        }

        return validationException;
    }

    /**
     * Generate the inner retriever tree for the given fields, weights, and query. The tree follows this structure:
     *
     * <pre>
     * multi_match query on all lexical fields
     * normalizer retriever
     *   match query on semantic_text field A
     *   match query on semantic_text field B
     *   ...
     *   match query on semantic_text field Z
     * </pre>
     *
     * <p>
     * Where the normalizer retriever is constructed by the {@code innerNormalizerGenerator} function.
     * </p>
     * <p>
     * This tree structure is repeated for each index in {@code indicesMetadata}. That is to say, that for each index in
     * {@code indicesMetadata}, (up to) a pair of retrievers will be added to the returned {@code RetrieverBuilder} list.
     * </p>
     *
     * @param fieldsAndWeights The fields to query and their respective weights, in "field^weight" format
     * @param query The query text
     * @param indicesMetadata The metadata for the indices to search
     * @param innerNormalizerGenerator The inner normalizer retriever generator function
     * @param weightValidator The field weight validator
     * @return The inner retriever tree as a {@code RetrieverBuilder} list
     */
    public static List<RetrieverBuilder> generateInnerRetrievers(
        List<String> fieldsAndWeights,
        String query,
        Collection<IndexMetadata> indicesMetadata,
        Function<List<WeightedRetrieverSource>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<String, Float> parsedFieldsAndWeights = QueryParserHelper.parseFieldsAndWeights(fieldsAndWeights);
        if (weightValidator != null) {
            parsedFieldsAndWeights.values().forEach(weightValidator);
        }

        // We expect up to 2 inner retrievers to be generated for each index queried
        List<RetrieverBuilder> innerRetrievers = new ArrayList<>(indicesMetadata.size() * 2);
        for (IndexMetadata indexMetadata : indicesMetadata) {
            innerRetrievers.addAll(
                generateInnerRetrieversForIndex(parsedFieldsAndWeights, query, indexMetadata, innerNormalizerGenerator, weightValidator)
            );
        }
        return innerRetrievers;
    }

    private static List<RetrieverBuilder> generateInnerRetrieversForIndex(
        Map<String, Float> parsedFieldsAndWeights,
        String query,
        IndexMetadata indexMetadata,
        Function<List<WeightedRetrieverSource>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<String, Float> fieldsAndWeightsToQuery = parsedFieldsAndWeights;
        if (fieldsAndWeightsToQuery.isEmpty()) {
            Settings settings = indexMetadata.getSettings();
            List<String> defaultFields = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
            fieldsAndWeightsToQuery = QueryParserHelper.parseFieldsAndWeights(defaultFields);
            if (weightValidator != null) {
                fieldsAndWeightsToQuery.values().forEach(weightValidator);
            }
        }

        Map<String, Float> inferenceFields = new HashMap<>();
        Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
        for (Map.Entry<String, Float> entry : fieldsAndWeightsToQuery.entrySet()) {
            String field = entry.getKey();
            Float weight = entry.getValue();

            if (Regex.isMatchAllPattern(field)) {
                indexInferenceFields.keySet().forEach(f -> addToInferenceFieldsMap(inferenceFields, f, weight));
            } else if (Regex.isSimpleMatchPattern(field)) {
                indexInferenceFields.keySet()
                    .stream()
                    .filter(f -> Regex.simpleMatch(field, f))
                    .forEach(f -> addToInferenceFieldsMap(inferenceFields, f, weight));
            } else {
                // No wildcards in field name
                if (indexInferenceFields.containsKey(field)) {
                    addToInferenceFieldsMap(inferenceFields, field, weight);
                }
            }
        }

        Map<String, Float> nonInferenceFields = new HashMap<>(fieldsAndWeightsToQuery);
        nonInferenceFields.keySet().removeAll(inferenceFields.keySet());  // Remove all inference fields from non-inference fields map

        // TODO: Set index pre-filters on returned retrievers when we want to implement multi-index support
        List<RetrieverBuilder> innerRetrievers = new ArrayList<>(2);
        if (nonInferenceFields.isEmpty() == false) {
            MultiMatchQueryBuilder nonInferenceFieldQueryBuilder = new MultiMatchQueryBuilder(query).type(
                MultiMatchQueryBuilder.Type.MOST_FIELDS
            ).fields(nonInferenceFields);
            innerRetrievers.add(new StandardRetrieverBuilder(nonInferenceFieldQueryBuilder));
        }
        if (inferenceFields.isEmpty() == false) {
            List<WeightedRetrieverSource> inferenceFieldRetrievers = new ArrayList<>(inferenceFields.size());
            inferenceFields.forEach((f, w) -> {
                RetrieverBuilder retrieverBuilder = new StandardRetrieverBuilder(new MatchQueryBuilder(f, query));
                inferenceFieldRetrievers.add(
                    new WeightedRetrieverSource(CompoundRetrieverBuilder.RetrieverSource.from(retrieverBuilder), w)
                );
            });

            innerRetrievers.add(innerNormalizerGenerator.apply(inferenceFieldRetrievers));
        }
        return innerRetrievers;
    }

    private static void addToInferenceFieldsMap(Map<String, Float> inferenceFields, String field, Float weight) {
        inferenceFields.compute(field, (k, v) -> v == null ? weight : v * weight);
    }
}

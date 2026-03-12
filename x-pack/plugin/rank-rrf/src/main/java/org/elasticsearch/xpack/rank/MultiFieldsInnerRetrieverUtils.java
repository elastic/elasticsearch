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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
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
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.cluster.metadata.IndexMetadata.getMatchingInferenceFields;
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
        @Nullable List<String> fields,
        @Nullable String query,
        String retrieverName,
        String retrieversParamName,
        String fieldsParamName,
        String queryParamName,
        ActionRequestValidationException validationException
    ) {
        if (fields != null || query != null) {
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

            if (fields != null && fields.isEmpty()) {
                validationException = addValidationError(
                    String.format(Locale.ROOT, "[%s] [%s] cannot be empty", retrieverName, fieldsParamName),
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
     * standard retriever for querying lexical fields using multi_match.
     * normalizer retriever
     *   match query on semantic_text field A with inference ID id1
     *   match query on semantic_text field A with inference ID id2
     *   match query on semantic_text field B with inference ID id1
     *   ...
     *   match query on semantic_text field Z with inference ID idN
     * </pre>
     *
     * <p>
     * Where the normalizer retriever is constructed by the {@code innerNormalizerGenerator} function.
     * </p>
     *
     * <p>
     * When the same lexical fields are queried for all indices, we use a single multi_match query to query them.
     * Otherwise, we create a boolean query with the following structure:
     * </p>
     *
     * <pre>
     * bool
     *   should
     *     bool
     *       match query on lexical fields for index A
     *       filter on indexA
     *     bool
     *       match query on lexical fields for index B
     *       filter on indexB
     *    ...
     * </pre>
     *
     * <p>
     * The semantic_text fields are grouped by inference ID. For each (fieldName, inferenceID) pair we generate a match query.
     * Since we have no way to effectively filter on inference IDs, we filter on index names instead.
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
        @Nullable List<String> fieldsAndWeights,
        String query,
        Collection<IndexMetadata> indicesMetadata,
        Function<List<WeightedRetrieverSource>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<String, Float> parsedFieldsAndWeights = fieldsAndWeights != null
            ? QueryParserHelper.parseFieldsAndWeights(fieldsAndWeights)
            : Map.of();
        if (weightValidator != null) {
            parsedFieldsAndWeights.values().forEach(weightValidator);
        }
        List<RetrieverBuilder> innerRetrievers = new ArrayList<>(2);
        // add lexical retriever
        RetrieverBuilder lexicalRetriever = generateLexicalRetriever(parsedFieldsAndWeights, indicesMetadata, query, weightValidator);
        if (lexicalRetriever != null) {
            innerRetrievers.add(lexicalRetriever);
        }
        // add semantic retriever
        RetrieverBuilder semanticRetriever = generateSemanticRetriever(
            parsedFieldsAndWeights,
            indicesMetadata,
            query,
            innerNormalizerGenerator,
            weightValidator
        );
        if (semanticRetriever != null) {
            innerRetrievers.add(semanticRetriever);
        }

        return innerRetrievers;
    }

    private static RetrieverBuilder generateSemanticRetriever(
        Map<String, Float> parsedFieldsAndWeights,
        Collection<IndexMetadata> indicesMetadata,
        String query,
        Function<List<WeightedRetrieverSource>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
        @Nullable Consumer<Float> weightValidator
    ) {
        // Form groups of (fieldName, inferenceID) that need to be queried.
        // For each (fieldName, inferenceID) pair determine the weight that needs to be applied and the indices that need to be queried.
        Map<Tuple<String, String>, List<String>> groupedIndices = new HashMap<>();
        Map<Tuple<String, String>, Float> groupedWeights = new HashMap<>();
        for (IndexMetadata indexMetadata : indicesMetadata) {
            inferenceFieldsAndWeightsForIndex(parsedFieldsAndWeights, indexMetadata, weightValidator).forEach((fieldName, weight) -> {
                String indexName = indexMetadata.getIndex().getName();
                Tuple<String, String> fieldAndInferenceId = new Tuple<>(
                    fieldName,
                    indexMetadata.getInferenceFields().get(fieldName).getInferenceId()
                );

                List<String> existingIndexNames = groupedIndices.get(fieldAndInferenceId);
                if (existingIndexNames != null && groupedWeights.get(fieldAndInferenceId).equals(weight) == false) {
                    String conflictingIndexName = existingIndexNames.getFirst();
                    throw new IllegalArgumentException(
                        "field [" + fieldName + "] has different weights in indices [" + conflictingIndexName + "] and [" + indexName + "]"
                    );
                }

                groupedWeights.put(fieldAndInferenceId, weight);
                groupedIndices.computeIfAbsent(fieldAndInferenceId, k -> new ArrayList<>()).add(indexName);
            });
        }

        // there are no semantic_text fields that need to be queried, no need to create a retriever
        if (groupedIndices.isEmpty()) {
            return null;
        }

        // for each (fieldName, inferenceID) pair generate a standard retriever with a semantic query
        List<WeightedRetrieverSource> semanticRetrievers = new ArrayList<>(groupedIndices.size());
        groupedIndices.forEach((fieldAndInferenceId, indexNames) -> {
            String fieldName = fieldAndInferenceId.v1();
            Float weight = groupedWeights.get(fieldAndInferenceId);

            QueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, query);

            // if indices does not contain all index names, we need to add a filter
            if (indicesMetadata.size() != indexNames.size()) {
                queryBuilder = new BoolQueryBuilder().must(queryBuilder).filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indexNames));
            }

            RetrieverBuilder retrieverBuilder = new StandardRetrieverBuilder(queryBuilder);
            semanticRetrievers.add(new WeightedRetrieverSource(CompoundRetrieverBuilder.RetrieverSource.from(retrieverBuilder), weight));
        });

        return innerNormalizerGenerator.apply(semanticRetrievers);
    }

    private static Map<String, Float> defaultFieldsAndWeightsForIndex(
        IndexMetadata indexMetadata,
        @Nullable Consumer<Float> weightValidator
    ) {
        Settings settings = indexMetadata.getSettings();
        List<String> defaultFields = settings.getAsList(DEFAULT_FIELD_SETTING.getKey(), DEFAULT_FIELD_SETTING.getDefault(settings));
        Map<String, Float> fieldsAndWeights = QueryParserHelper.parseFieldsAndWeights(defaultFields);
        if (weightValidator != null) {
            fieldsAndWeights.values().forEach(weightValidator);
        }
        return fieldsAndWeights;
    }

    private static Map<String, Float> inferenceFieldsAndWeightsForIndex(
        Map<String, Float> parsedFieldsAndWeights,
        IndexMetadata indexMetadata,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<String, Float> fieldsAndWeightsToQuery = parsedFieldsAndWeights;
        if (fieldsAndWeightsToQuery.isEmpty()) {
            fieldsAndWeightsToQuery = defaultFieldsAndWeightsForIndex(indexMetadata, weightValidator);
        }

        Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
        return getMatchingInferenceFields(indexInferenceFields, fieldsAndWeightsToQuery, true).entrySet()
            .stream()
            .collect(Collectors.toMap(e -> e.getKey().getName(), Map.Entry::getValue));
    }

    private static Map<String, Float> nonInferenceFieldsAndWeightsForIndex(
        Map<String, Float> fieldsAndWeightsToQuery,
        IndexMetadata indexMetadata,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<String, Float> nonInferenceFields = new HashMap<>(fieldsAndWeightsToQuery);

        if (nonInferenceFields.isEmpty()) {
            nonInferenceFields = defaultFieldsAndWeightsForIndex(indexMetadata, weightValidator);
        }

        nonInferenceFields.keySet().removeAll(indexMetadata.getInferenceFields().keySet());
        return nonInferenceFields;
    }

    private static RetrieverBuilder generateLexicalRetriever(
        Map<String, Float> fieldsAndWeightsToQuery,
        Collection<IndexMetadata> indicesMetadata,
        String query,
        @Nullable Consumer<Float> weightValidator
    ) {
        Map<Map<String, Float>, List<String>> groupedIndices = new HashMap<>();

        for (IndexMetadata indexMetadata : indicesMetadata) {
            Map<String, Float> nonInferenceFieldsForIndex = nonInferenceFieldsAndWeightsForIndex(
                fieldsAndWeightsToQuery,
                indexMetadata,
                weightValidator
            );

            if (nonInferenceFieldsForIndex.isEmpty()) {
                continue;
            }

            groupedIndices.computeIfAbsent(nonInferenceFieldsForIndex, k -> new ArrayList<>()).add(indexMetadata.getIndex().getName());
        }

        // there are no lexical fields that need to be queried, no need to create a retriever
        if (groupedIndices.isEmpty()) {
            return null;
        }

        List<QueryBuilder> lexicalQueryBuilders = new ArrayList<>();
        for (var entry : groupedIndices.entrySet()) {
            Map<String, Float> fieldsAndWeights = entry.getKey();
            List<String> indices = entry.getValue();

            QueryBuilder queryBuilder = new MultiMatchQueryBuilder(query).type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
                .fields(fieldsAndWeights);

            // if indices does not contain all index names, we need to add a filter
            if (indices.size() != indicesMetadata.size()) {
                queryBuilder = new BoolQueryBuilder().must(queryBuilder).filter(new TermsQueryBuilder(IndexFieldMapper.NAME, indices));
            }

            lexicalQueryBuilders.add(queryBuilder);
        }

        // only a single lexical query, no need to wrap in a boolean query
        if (lexicalQueryBuilders.size() == 1) {
            return new StandardRetrieverBuilder(lexicalQueryBuilders.getFirst());
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        lexicalQueryBuilders.forEach(boolQueryBuilder::should);
        return new StandardRetrieverBuilder(boolQueryBuilder);
    }
}

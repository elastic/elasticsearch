/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.simplified;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.search.QueryParserHelper;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static org.elasticsearch.index.IndexSettings.DEFAULT_FIELD_SETTING;

public class SimplifiedInnerRetrieverUtils {
    private static final String ALL_FIELDS_WILDCARD = "*";

    private SimplifiedInnerRetrieverUtils() {}

    public static List<RetrieverBuilder> generateInnerRetrievers(
        List<String> fieldsAndWeights,
        String query,
        Collection<IndexMetadata> indicesMetadata,
        BiFunction<List<CompoundRetrieverBuilder.RetrieverSource>, List<Float>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
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
        BiFunction<List<CompoundRetrieverBuilder.RetrieverSource>, List<Float>, CompoundRetrieverBuilder<?>> innerNormalizerGenerator,
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

        if (fieldsAndWeightsToQuery.size() == 1 && fieldsAndWeightsToQuery.get(ALL_FIELDS_WILDCARD) != null) {
            // TODO: Implement support for this case
            throw new UnsupportedOperationException("All fields wildcard is not supported yet");
        }

        // TODO: Should we use a separate match query for each non-inference field, perform secondary normalization,
        // and apply the boost after secondary normalization, like is done for inference fields?
        List<CompoundRetrieverBuilder.RetrieverSource> inferenceFieldRetrievers = new ArrayList<>();
        List<Float> inferenceFieldWeights = new ArrayList<>();
        MultiMatchQueryBuilder nonInferenceFieldQueryBuilder = new MultiMatchQueryBuilder(query);

        Map<String, InferenceFieldMetadata> indexInferenceFields = indexMetadata.getInferenceFields();
        for (Map.Entry<String, Float> entry : fieldsAndWeightsToQuery.entrySet()) {
            String field = entry.getKey();
            Float weight = entry.getValue();

            InferenceFieldMetadata inferenceFieldMetadata = indexInferenceFields.get(field);
            if (inferenceFieldMetadata != null) {
                RetrieverBuilder retrieverBuilder = new StandardRetrieverBuilder(new MatchQueryBuilder(field, query));
                inferenceFieldRetrievers.add(new CompoundRetrieverBuilder.RetrieverSource(retrieverBuilder, null));
                inferenceFieldWeights.add(weight);
            } else {
                nonInferenceFieldQueryBuilder.field(field, weight);
            }
        }

        // TODO: Set index pre-filters on returned retrievers when we want to implement multi-index support
        List<RetrieverBuilder> innerRetrievers = new ArrayList<>(2);
        if (nonInferenceFieldQueryBuilder.fields().isEmpty() == false) {
            innerRetrievers.add(new StandardRetrieverBuilder(nonInferenceFieldQueryBuilder));
        }
        if (inferenceFieldRetrievers.isEmpty() == false) {
            innerRetrievers.add(innerNormalizerGenerator.apply(inferenceFieldRetrievers, inferenceFieldWeights));
        }
        return innerRetrievers;
    }
}

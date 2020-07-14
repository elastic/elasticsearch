/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.ml.dataframe.traintestsplit.TrainTestSplitterFactory;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class DataFrameDataExtractorFactory {

    private final Client client;
    private final String analyticsId;
    private final List<String> indices;
    private final QueryBuilder sourceQuery;
    private final ExtractedFields extractedFields;
    private final List<RequiredField> requiredFields;
    private final Map<String, String> headers;
    private final boolean supportsRowsWithMissingValues;
    private final TrainTestSplitterFactory trainTestSplitterFactory;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, List<String> indices, QueryBuilder sourceQuery,
                                          ExtractedFields extractedFields, List<RequiredField> requiredFields, Map<String, String> headers,
                                          boolean supportsRowsWithMissingValues,
                                          TrainTestSplitterFactory trainTestSplitterFactory) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.indices = Objects.requireNonNull(indices);
        this.sourceQuery = Objects.requireNonNull(sourceQuery);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.requiredFields = Objects.requireNonNull(requiredFields);
        this.headers = headers;
        this.supportsRowsWithMissingValues = supportsRowsWithMissingValues;
        this.trainTestSplitterFactory = Objects.requireNonNull(trainTestSplitterFactory);
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                indices,
                buildQuery(),
                1000,
                headers,
                includeSource,
                supportsRowsWithMissingValues,
                trainTestSplitterFactory
            );
        return new DataFrameDataExtractor(client, context);
    }

    private QueryBuilder buildQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery().filter(sourceQuery);
        requiredFields.forEach(requiredField -> query.filter(QueryBuilders.existsQuery(requiredField.getName())));
        return query;
    }

    public ExtractedFields getExtractedFields() {
        return extractedFields;
    }

    /**
     * Create a new extractor factory
     *
     * The source index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param taskId The task id
     * @param config The config from which to create the extractor factory
     * @param extractedFields The fields to extract
     */
    public static DataFrameDataExtractorFactory createForSourceIndices(Client client, String taskId, DataFrameAnalyticsConfig config,
                                                                       ExtractedFields extractedFields) {
        return new DataFrameDataExtractorFactory(client, taskId, Arrays.asList(config.getSource().getIndex()),
            config.getSource().getParsedQuery(), extractedFields, config.getAnalysis().getRequiredFields(), config.getHeaders(),
            config.getAnalysis().supportsMissingValues(), createTrainTestSplitterFactory(client, config, extractedFields));
    }

    private static TrainTestSplitterFactory createTrainTestSplitterFactory(Client client, DataFrameAnalyticsConfig config,
                                                                           ExtractedFields extractedFields) {
        return new TrainTestSplitterFactory(client, config,
            extractedFields.getAllFields().stream().map(ExtractedField::getName).collect(Collectors.toList()));
    }

    /**
     * Validate and create a new extractor factory
     *
     * The destination index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param config The config from which to create the extractor factory
     * @param listener The listener to notify on creation or failure
     */
    public static void createForDestinationIndex(Client client,
                                                 DataFrameAnalyticsConfig config,
                                                 ActionListener<DataFrameDataExtractorFactory> listener) {
        ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory = new ExtractedFieldsDetectorFactory(client);
        extractedFieldsDetectorFactory.createFromDest(config, ActionListener.wrap(
            extractedFieldsDetector -> {
                ExtractedFields extractedFields = extractedFieldsDetector.detect().v1();

                DataFrameDataExtractorFactory extractorFactory = new DataFrameDataExtractorFactory(client, config.getId(),
                    Collections.singletonList(config.getDest().getIndex()), config.getSource().getParsedQuery(), extractedFields,
                    config.getAnalysis().getRequiredFields(), config.getHeaders(), config.getAnalysis().supportsMissingValues(),
                    createTrainTestSplitterFactory(client, config, extractedFields));
                listener.onResponse(extractorFactory);
            },
            listener::onFailure
        ));
    }
}

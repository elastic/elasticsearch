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
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameDataExtractorFactory {

    private final Client client;
    private final String analyticsId;
    private final List<String> indices;
    private final ExtractedFields extractedFields;
    private final Map<String, String> headers;
    private final boolean includeRowsWithMissingValues;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, List<String> indices, ExtractedFields extractedFields,
                                          Map<String, String> headers, boolean includeRowsWithMissingValues) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.indices = Objects.requireNonNull(indices);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.headers = headers;
        this.includeRowsWithMissingValues = includeRowsWithMissingValues;
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                indices,
                createQuery(),
                1000,
                headers,
                includeSource,
                includeRowsWithMissingValues
            );
        return new DataFrameDataExtractor(client, context);
    }

    private QueryBuilder createQuery() {
        return includeRowsWithMissingValues ? QueryBuilders.matchAllQuery() : allExtractedFieldsExistQuery();
    }

    private QueryBuilder allExtractedFieldsExistQuery() {
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (ExtractedField field : extractedFields.getAllFields()) {
            query.filter(QueryBuilders.existsQuery(field.getName()));
        }
        return query;
    }

    /**
     * Validate and create a new extractor factory
     *
     * The source index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param taskId The task id
     * @param isTaskRestarting Whether the task is restarting or it is running for the first time
     * @param config The config from which to create the extractor factory
     * @param listener The listener to notify on creation or failure
     */
    public static void createForSourceIndices(Client client,
                                              String taskId,
                                              boolean isTaskRestarting,
                                              DataFrameAnalyticsConfig config,
                                              ActionListener<DataFrameDataExtractorFactory> listener) {
        ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory = new ExtractedFieldsDetectorFactory(client);
        extractedFieldsDetectorFactory.createFromSource(config, isTaskRestarting, ActionListener.wrap(
            extractedFieldsDetector -> {
                ExtractedFields extractedFields = extractedFieldsDetector.detect();
                DataFrameDataExtractorFactory extractorFactory = new DataFrameDataExtractorFactory(client, taskId,
                    Arrays.asList(config.getSource().getIndex()), extractedFields, config.getHeaders(),
                    config.getAnalysis().supportsMissingValues());
                listener.onResponse(extractorFactory);
            },
            listener::onFailure
        ));
    }

    /**
     * Validate and create a new extractor factory
     *
     * The destination index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param config The config from which to create the extractor factory
     * @param isTaskRestarting Whether the task is restarting
     * @param listener The listener to notify on creation or failure
     */
    public static void createForDestinationIndex(Client client,
                                                 DataFrameAnalyticsConfig config,
                                                 boolean isTaskRestarting,
                                                 ActionListener<DataFrameDataExtractorFactory> listener) {
        ExtractedFieldsDetectorFactory extractedFieldsDetectorFactory = new ExtractedFieldsDetectorFactory(client);
        extractedFieldsDetectorFactory.createFromDest(config, isTaskRestarting, ActionListener.wrap(
            extractedFieldsDetector -> {
                ExtractedFields extractedFields = extractedFieldsDetector.detect();
                DataFrameDataExtractorFactory extractorFactory = new DataFrameDataExtractorFactory(client, config.getId(),
                    Arrays.asList(config.getSource().getIndex()), extractedFields, config.getHeaders(),
                    config.getAnalysis().supportsMissingValues());
                listener.onResponse(extractorFactory);
            },
            listener::onFailure
        ));
    }
}

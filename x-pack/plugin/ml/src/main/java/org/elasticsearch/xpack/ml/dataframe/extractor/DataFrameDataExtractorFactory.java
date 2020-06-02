/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameDataExtractorFactory {

    private final Client client;
    private final String analyticsId;
    private final List<String> indices;
    private final QueryBuilder sourceQuery;
    private final ExtractedFields extractedFields;
    private final Map<String, String> headers;
    private final boolean supportsRowsWithMissingValues;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, List<String> indices, QueryBuilder sourceQuery,
                                          ExtractedFields extractedFields, Map<String, String> headers,
                                          boolean supportsRowsWithMissingValues) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.indices = Objects.requireNonNull(indices);
        this.sourceQuery = Objects.requireNonNull(sourceQuery);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.headers = headers;
        this.supportsRowsWithMissingValues = supportsRowsWithMissingValues;
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                indices,
                QueryBuilders.boolQuery().filter(sourceQuery),
                1000,
                headers,
                includeSource,
                supportsRowsWithMissingValues
            );
        return new DataFrameDataExtractor(client, context);
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
            config.getSource().getParsedQuery(), extractedFields, config.getHeaders(), config.getAnalysis().supportsMissingValues());
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
                    config.getHeaders(), config.getAnalysis().supportsMissingValues());
                listener.onResponse(extractorFactory);
            },
            listener::onFailure
        ));
    }
}

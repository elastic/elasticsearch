/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.ml.dataframe.traintestsplit.TrainTestSplitterFactory;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataFrameDataExtractorContext {

    final String jobId;
    final ExtractedFields extractedFields;
    final String[] indices;
    final QueryBuilder query;
    final int scrollSize;
    final Map<String, String> headers;
    final boolean includeSource;
    final boolean supportsRowsWithMissingValues;
    final TrainTestSplitterFactory trainTestSplitterFactory;

    // Runtime mappings are necessary while we are still querying the source indices.
    // They should be empty when we're querying the destination index as the runtime
    // fields should be mapped in the index.
    final Map<String, Object> runtimeMappings;

    DataFrameDataExtractorContext(String jobId, ExtractedFields extractedFields, List<String> indices, QueryBuilder query, int scrollSize,
                                  Map<String, String> headers, boolean includeSource, boolean supportsRowsWithMissingValues,
                                  TrainTestSplitterFactory trainTestSplitterFactory, Map<String, Object> runtimeMappings) {
        this.jobId = Objects.requireNonNull(jobId);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.indices = indices.toArray(new String[indices.size()]);
        this.query = Objects.requireNonNull(query);
        this.scrollSize = scrollSize;
        this.headers = headers;
        this.includeSource = includeSource;
        this.supportsRowsWithMissingValues = supportsRowsWithMissingValues;
        this.trainTestSplitterFactory = Objects.requireNonNull(trainTestSplitterFactory);
        this.runtimeMappings = Objects.requireNonNull(runtimeMappings);
    }
}

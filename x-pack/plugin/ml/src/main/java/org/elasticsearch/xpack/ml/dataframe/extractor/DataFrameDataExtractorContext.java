/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.elasticsearch.index.query.QueryBuilder;
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

    DataFrameDataExtractorContext(String jobId, ExtractedFields extractedFields, List<String> indices, QueryBuilder query, int scrollSize,
                                  Map<String, String> headers, boolean includeSource, boolean supportsRowsWithMissingValues) {
        this.jobId = Objects.requireNonNull(jobId);
        this.extractedFields = Objects.requireNonNull(extractedFields);
        this.indices = indices.toArray(new String[indices.size()]);
        this.query = Objects.requireNonNull(query);
        this.scrollSize = scrollSize;
        this.headers = headers;
        this.includeSource = includeSource;
        this.supportsRowsWithMissingValues = supportsRowsWithMissingValues;
    }
}

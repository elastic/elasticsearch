/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class AnalyticsProcessConfig implements ToXContentObject {

    private static final String JOB_ID = "job_id";
    private static final String ROWS = "rows";
    private static final String COLS = "cols";
    private static final String MEMORY_LIMIT = "memory_limit";
    private static final String THREADS = "threads";
    private static final String ANALYSIS = "analysis";
    private static final String RESULTS_FIELD = "results_field";
    private static final String CATEGORICAL_FIELDS = "categorical_fields";

    private final String jobId;
    private final long rows;
    private final int cols;
    private final ByteSizeValue memoryLimit;
    private final int threads;
    private final String resultsField;
    private final Set<String> categoricalFields;
    private final DataFrameAnalysis analysis;
    private final ExtractedFields extractedFields;

    public AnalyticsProcessConfig(String jobId, long rows, int cols, ByteSizeValue memoryLimit, int threads, String resultsField,
                                  Set<String> categoricalFields, DataFrameAnalysis analysis, ExtractedFields extractedFields) {
        this.jobId = Objects.requireNonNull(jobId);
        this.rows = rows;
        this.cols = cols;
        this.memoryLimit = Objects.requireNonNull(memoryLimit);
        this.threads = threads;
        this.resultsField = Objects.requireNonNull(resultsField);
        this.categoricalFields = Objects.requireNonNull(categoricalFields);
        this.analysis = Objects.requireNonNull(analysis);
        this.extractedFields = Objects.requireNonNull(extractedFields);
    }

    public String jobId() {
        return jobId;
    }

    public long rows() {
        return rows;
    }

    public int cols() {
        return cols;
    }

    public int threads() {
        return threads;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(JOB_ID, jobId);
        builder.field(ROWS, rows);
        builder.field(COLS, cols);
        builder.field(MEMORY_LIMIT, memoryLimit.getBytes());
        builder.field(THREADS, threads);
        builder.field(RESULTS_FIELD, resultsField);
        builder.field(CATEGORICAL_FIELDS, categoricalFields);
        builder.field(ANALYSIS, new DataFrameAnalysisWrapper(analysis, extractedFields));
        builder.endObject();
        return builder;
    }

    private static class DataFrameAnalysisWrapper implements ToXContentObject {

        private final DataFrameAnalysis analysis;
        private final ExtractedFields extractedFields;

        private DataFrameAnalysisWrapper(DataFrameAnalysis analysis, ExtractedFields extractedFields) {
            this.analysis = analysis;
            this.extractedFields = extractedFields;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", analysis.getWriteableName());
            builder.field("parameters", analysis.getParams(new AnalysisFieldInfo(extractedFields)));
            builder.endObject();
            return builder;
        }
    }
}

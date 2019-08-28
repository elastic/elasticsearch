/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class AnalyticsProcessConfig implements ToXContentObject {

    private static final String ROWS = "rows";
    private static final String COLS = "cols";
    private static final String MEMORY_LIMIT = "memory_limit";
    private static final String THREADS = "threads";
    private static final String ANALYSIS = "analysis";
    private static final String RESULTS_FIELD = "results_field";
    private static final String CATEGORICAL_FIELDS = "categorical_fields";

    private final long rows;
    private final int cols;
    private final ByteSizeValue memoryLimit;
    private final int threads;
    private final String resultsField;
    private final Set<String> categoricalFields;
    private final DataFrameAnalysis analysis;

    public AnalyticsProcessConfig(long rows, int cols, ByteSizeValue memoryLimit, int threads, String resultsField,
                                  Set<String> categoricalFields, DataFrameAnalysis analysis) {
        this.rows = rows;
        this.cols = cols;
        this.memoryLimit = Objects.requireNonNull(memoryLimit);
        this.threads = threads;
        this.resultsField = Objects.requireNonNull(resultsField);
        this.categoricalFields = Objects.requireNonNull(categoricalFields);
        this.analysis = Objects.requireNonNull(analysis);
    }

    public long rows() {
        return rows;
    }

    public int cols() {
        return cols;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ROWS, rows);
        builder.field(COLS, cols);
        builder.field(MEMORY_LIMIT, memoryLimit.getBytes());
        builder.field(THREADS, threads);
        builder.field(RESULTS_FIELD, resultsField);
        builder.field(CATEGORICAL_FIELDS, categoricalFields);
        builder.field(ANALYSIS, new DataFrameAnalysisWrapper(analysis));
        builder.endObject();
        return builder;
    }

    private static class DataFrameAnalysisWrapper implements ToXContentObject {

        private final DataFrameAnalysis analysis;

        private DataFrameAnalysisWrapper(DataFrameAnalysis analysis) {
            this.analysis = analysis;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("name", analysis.getWriteableName());
            builder.field("parameters", analysis.getParams());
            builder.endObject();
            return builder;
        }
    }
}

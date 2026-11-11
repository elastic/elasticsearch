/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.ml.datafeed.delayeddatacheck.AbstractDelayedDataDetector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.IDENTIFIER;
import static org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder.EsqlQueryParam.ParamClassification.VALUE;
import static org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig.MAX_NUMBER_SPANABLE_BUCKETS;

/** Delayed-data detector for ES|QL datafeeds: one aggregate query per check window. */
public class EsqlDelayedDataDetector extends AbstractDelayedDataDetector {

    static final String ML_COUNT_COLUMN = "__ml_count";
    static final String ML_BUCKET_COLUMN = "__ml_bucket";

    // Without a LIMIT, ES|QL truncates at result_truncation_default_size (1000) rows and reports it only in a Warning header.
    // A custom check_window can span up to MAX_NUMBER_SPANABLE_BUCKETS buckets (one row each). A dropped bucket hides its delayed data.
    private static final String BUCKET_EVENT_COUNT_SUFFIX = Strings.format("""

        | STATS %s = SUM(??countField) BY %s = TO_LONG(TO_DATETIME(??timeField)) / ?bucketSpan
        | LIMIT %d""", ML_COUNT_COLUMN, ML_BUCKET_COLUMN, MAX_NUMBER_SPANABLE_BUCKETS);

    private final String emittedTimeField;
    private final String summaryCountFieldName;
    private final String esqlQuery;
    @Nullable
    private final String projectRouting;
    private final String sourceTimeField;
    private final EsqlSyncQueryRunner queryRunner;

    public EsqlDelayedDataDetector(
        long bucketSpan,
        long window,
        String jobId,
        String emittedTimeField,
        String summaryCountFieldName,
        String esqlQuery,
        Map<String, String> headers,
        @Nullable String projectRouting,
        String sourceTimeField,
        Client client
    ) {
        this(
            bucketSpan,
            window,
            jobId,
            emittedTimeField,
            summaryCountFieldName,
            esqlQuery,
            headers,
            projectRouting,
            sourceTimeField,
            client,
            (query, filter, params) -> EsqlQueryExecutor.executeSync(
                client,
                EsqlQueryExecutor.newRequest(client, query, filter, params, projectRouting),
                headers
            )
        );
    }

    EsqlDelayedDataDetector(
        long bucketSpan,
        long window,
        String jobId,
        String emittedTimeField,
        String summaryCountFieldName,
        String esqlQuery,
        Map<String, String> headers,
        @Nullable String projectRouting,
        String sourceTimeField,
        Client client,
        EsqlSyncQueryRunner queryRunner
    ) {
        super(bucketSpan, window, Objects.requireNonNull(jobId), Objects.requireNonNull(client));
        assert bucketSpan > 0 : "bucketSpan must be positive";
        this.emittedTimeField = Objects.requireNonNull(emittedTimeField);
        this.summaryCountFieldName = Objects.requireNonNull(summaryCountFieldName);
        this.esqlQuery = Objects.requireNonNull(esqlQuery);
        this.projectRouting = projectRouting;
        this.sourceTimeField = Objects.requireNonNull(sourceTimeField);
        this.queryRunner = Objects.requireNonNull(queryRunner);
    }

    static String bucketEventCountQuery(String userQuery) {
        return userQuery + BUCKET_EVENT_COUNT_SUFFIX;
    }

    @Override
    protected Map<Long, Long> currentBucketEventCounts(long start, long end) {
        String query = bucketEventCountQuery(esqlQuery);
        QueryBuilder filter = EsqlDataExtractorQueryUtils.timeRangeFilter(sourceTimeField, start, end);
        List<EsqlQueryParam> params = List.of(
            new EsqlQueryParam("countField", summaryCountFieldName, IDENTIFIER),
            new EsqlQueryParam("timeField", emittedTimeField, IDENTIFIER),
            new EsqlQueryParam("bucketSpan", bucketSpan, VALUE)
        );
        try (EsqlQueryResponse response = EsqlQueryRunner.runWithSingleRetry(queryRunner, query, filter, params, () -> false, jobId)) {
            return parseBucketCounts(response.response());
        }
    }

    private Map<Long, Long> parseBucketCounts(EsqlResponse esqlResponse) {
        ColumnIndexes columnIndexes = columnIndexes(esqlResponse.columns());
        Map<Long, Long> bucketCounts = new HashMap<>();
        for (Iterable<Object> row : esqlResponse.rows()) {
            accumulateRow(bucketCounts, row, columnIndexes);
        }
        return bucketCounts;
    }

    private void accumulateRow(Map<Long, Long> bucketCounts, Iterable<Object> row, ColumnIndexes columnIndexes) {
        List<Object> values = new ArrayList<>();
        for (Object value : row) {
            values.add(value);
        }
        Object bucketKey = values.get(columnIndexes.bucketColumnIndex());
        if (bucketKey == null) {
            return;
        }
        long bucketIndex = toLong(bucketKey);
        long count = toLong(values.get(columnIndexes.countColumnIndex()));
        bucketCounts.merge(bucketIndex * bucketSpan, count, Long::sum);
    }

    private ColumnIndexes columnIndexes(List<? extends ColumnInfo> columns) {
        int countColumnIndex = -1;
        int bucketColumnIndex = -1;
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            if (ML_COUNT_COLUMN.equals(name)) {
                countColumnIndex = i;
            } else if (ML_BUCKET_COLUMN.equals(name)) {
                bucketColumnIndex = i;
            }
        }
        if (countColumnIndex < 0 || bucketColumnIndex < 0) {
            throw new IllegalStateException(
                "[" + jobId + "] Delayed data check expected columns [" + ML_COUNT_COLUMN + "] and [" + ML_BUCKET_COLUMN + "]"
            );
        }
        return new ColumnIndexes(countColumnIndex, bucketColumnIndex);
    }

    private record ColumnIndexes(int countColumnIndex, int bucketColumnIndex) {}

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalStateException("Expected a numeric ES|QL value but got [" + value + "]");
    }
}

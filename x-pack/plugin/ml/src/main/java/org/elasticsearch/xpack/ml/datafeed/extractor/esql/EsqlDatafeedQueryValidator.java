/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.crossproject.NoMatchingProjectException;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

/**
 * Includes helper functions for validating the ESQL query provided to a datafeed.
 */
public class EsqlDatafeedQueryValidator {

    /**
     * Returns the summary count field name that the ESQL query must output, or {@code null} if it is
     * not required. The field is only required when the job configures a {@code summary_count_field_name}
     * and the datafeed's delayed data check is enabled.
     */
    public static String requiredSummaryCountField(DatafeedConfig datafeed, Job job) {
        String summaryCountField = job.getAnalysisConfig().getSummaryCountFieldName();
        DelayedDataCheckConfig delayedDataCheckConfig = datafeed.getDelayedDataCheckConfig();
        boolean delayedDataCheckEnabled = delayedDataCheckConfig != null && delayedDataCheckConfig.isEnabled();
        return (Strings.hasText(summaryCountField) && delayedDataCheckEnabled) ? summaryCountField : null;
    }

    /**
     * Validates an ESQL datafeed query by executing {@code esqlQuery | LIMIT 0} under the supplied
     * security headers. This surfaces any query problem — invalid syntax, a query that fails to run,
     * or missing required output columns ({@code timeField} and, when non-null, {@code summaryCountField}).
     * Calls {@code listener.onResponse(true)} on success or when the target index does not exist;
     * calls {@code listener.onFailure} for all other problems.
     */
    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        String summaryCountField,
        ActionListener<Boolean> listener
    ) {
        validateQuery(client, headers, esqlQuery, projectRouting, timeField, summaryCountField, listener, null);
    }

    public void validateQuery(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        String timeField,
        String summaryCountField,
        ActionListener<Boolean> listener,
        @Nullable String datafeedId
    ) {
        warnForConflictingOuterClauses(datafeedId, esqlQuery, timeField);
        String limitZeroQuery = esqlQuery + " | LIMIT 0";

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> {
            try {
                checkRequiredColumns(response.response().columns(), timeField, summaryCountField, datafeedId);
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof NoMatchingProjectException || cause instanceof IndexNotFoundException) {
                // Deferred-existence cases: the project may be linked later or the index may not
                // exist yet. Skip the column check — there is nothing to validate against.
                listener.onResponse(Boolean.TRUE);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, responseListener);
    }

    /**
     * Warns about outer user pipeline clauses that conflict with the time range, order, and row cap owned by ML.
     */
    static void warnForConflictingOuterClauses(@Nullable String datafeedId, String esqlQuery, String timeField) {
        EsqlQueryClauseScanner.ScanResult scan = EsqlQueryClauseScanner.scan(esqlQuery, timeField);
        String datafeedContext = datafeedId == null ? "ES|QL datafeed query" : "ES|QL datafeed [" + datafeedId + "] query";
        if (scan.hasOuterTimeWhere()) {
            HeaderWarning.addWarning(
                datafeedContext
                    + " contains an outer WHERE clause on job time field ["
                    + timeField
                    + "]; remove the time-field WHERE clause because ML owns the request window."
            );
        }
        if (scan.hasOuterTimeSort()) {
            HeaderWarning.addWarning(
                datafeedContext
                    + " contains an outer SORT clause on job time field ["
                    + timeField
                    + "]; remove or change the time-field SORT clause because ML owns the request order."
            );
        }
        if (scan.hasOuterLimit()) {
            HeaderWarning.addWarning(datafeedContext + " contains an outer LIMIT clause; remove it because ML owns the safety ceiling.");
        }
    }

    /**
     * Probe run before minting a CPS internal credential: executes {@code esqlQuery | LIMIT 0} under
     * the caller's credential to confirm access. Does NOT check output columns (that is done by
     * {@link #validateQuery}). Tolerates {@link NoMatchingProjectException} (a project may be linked
     * later) and {@link IndexNotFoundException} (the index may be created later).
     * Calls {@code listener.onResponse(null)} on success or for those tolerated failures, and
     * {@code listener.onFailure} for all other problems.
     */
    public void validateAccessForMint(
        Client client,
        Map<String, String> headers,
        String esqlQuery,
        @Nullable String projectRouting,
        ActionListener<Void> listener
    ) {
        String limitZeroQuery = esqlQuery + " | LIMIT 0";

        ActionListener<EsqlQueryResponse> responseListener = ActionListener.wrap(response -> listener.onResponse(null), e -> {
            Throwable cause = ExceptionsHelper.unwrapCause(e);
            if (cause instanceof NoMatchingProjectException || cause instanceof IndexNotFoundException) {
                // Deferred-existence cases: the project may be linked later or the index may not
                // exist yet. Defer to runtime — consistent with the classic SearchRequest probe.
                listener.onResponse(null);
            } else {
                listener.onFailure(e);
            }
        });

        executeEsqlQueryAsync(client, limitZeroQuery, headers, projectRouting, responseListener);
    }

    static void validateEmittedTimesInSourceWindow(
        List<? extends ColumnInfo> columns,
        Iterable<? extends Iterable<Object>> rows,
        String jobId,
        String emittedTimeField,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        int emittedTimeColumnIndex = indexOfColumn(columns, emittedTimeField);
        boolean emittedTimeIsDate = isDateColumnType(columns.get(emittedTimeColumnIndex).outputType());
        for (Iterable<Object> row : rows) {
            validateEmittedTimeInSourceWindow(
                jobId,
                emittedTimeField,
                valueAt(row, emittedTimeColumnIndex),
                emittedTimeIsDate,
                sourceWindowStart,
                sourceWindowEnd
            );
        }
    }

    private static void validateEmittedTimeInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
        boolean emittedTimeIsDate,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        validateEmittedTimeValueInSourceWindow(jobId, emittedTimeField, rawValue, emittedTimeIsDate, sourceWindowStart, sourceWindowEnd);
    }

    /**
     * Validates and converts an emitted ES|QL timestamp that has already been materialized outside
     * the typed ES|QL response. String values are date/date_nanos representations; numeric values
     * are epoch milliseconds.
     */
    public static long validateEmittedTimeValueInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        return validateEmittedTimeValueInSourceWindow(
            jobId,
            emittedTimeField,
            rawValue,
            rawValue instanceof String,
            sourceWindowStart,
            sourceWindowEnd
        );
    }

    private static long validateEmittedTimeValueInSourceWindow(
        String jobId,
        String emittedTimeField,
        Object rawValue,
        boolean emittedTimeIsDate,
        long sourceWindowStart,
        long sourceWindowEnd
    ) {
        if (rawValue == null) {
            throw emittedTimeValidationException(
                jobId,
                emittedTimeField,
                "value is null",
                sourceWindowStart,
                sourceWindowEnd,
                "Ensure the ES|QL query returns a non-null scalar timestamp for every row"
            );
        }
        if (rawValue instanceof List<?>) {
            throw emittedTimeValidationException(
                jobId,
                emittedTimeField,
                "value is multi-valued",
                sourceWindowStart,
                sourceWindowEnd,
                "Ensure the emitted time field contains exactly one timestamp per row"
            );
        }
        final long emittedTimeMillis;
        try {
            emittedTimeMillis = toEpochMillis(rawValue, emittedTimeIsDate);
        } catch (RuntimeException e) {
            throw emittedTimeValidationException(
                jobId,
                emittedTimeField,
                "value has an unsupported type",
                sourceWindowStart,
                sourceWindowEnd,
                "Ensure the emitted time field is a date or numeric timestamp"
            );
        }
        if (emittedTimeMillis < sourceWindowStart) {
            throw emittedTimeValidationException(
                jobId,
                emittedTimeField,
                "value [" + emittedTimeMillis + "] is before the source window start",
                sourceWindowStart,
                sourceWindowEnd,
                "Check grouping alignment so emitted timestamps fall within the queried source range"
            );
        }
        if (emittedTimeMillis >= sourceWindowEnd) {
            throw emittedTimeValidationException(
                jobId,
                emittedTimeField,
                "value [" + emittedTimeMillis + "] is at or after the source window end",
                sourceWindowStart,
                sourceWindowEnd,
                "Check grouping alignment so emitted timestamps fall within the queried source range"
            );
        }
        return emittedTimeMillis;
    }

    private static IllegalArgumentException emittedTimeValidationException(
        String jobId,
        String emittedTimeField,
        String problem,
        long sourceWindowStart,
        long sourceWindowEnd,
        String correctiveAction
    ) {
        return new IllegalArgumentException(
            Messages.getMessage(
                Messages.DATAFEED_ESQL_EMITTED_TIME_VALIDATION_FAILED,
                jobId,
                emittedTimeField,
                problem,
                sourceWindowStart,
                sourceWindowEnd,
                correctiveAction
            )
        );
    }

    private static int indexOfColumn(List<? extends ColumnInfo> columns, String columnName) {
        for (int index = 0; index < columns.size(); index++) {
            if (columnName.equals(columns.get(index).name())) {
                return index;
            }
        }
        throw new IllegalArgumentException("ESQL query response is missing the required columns: " + columnName);
    }

    private static Object valueAt(Iterable<Object> row, int columnIndex) {
        Iterator<Object> values = row.iterator();
        for (int index = 0; index < columnIndex; index++) {
            if (values.hasNext() == false) {
                return null;
            }
            values.next();
        }
        return values.hasNext() ? values.next() : null;
    }

    private static boolean isDateColumnType(String outputType) {
        return "date".equals(outputType) || "date_nanos".equals(outputType);
    }

    private static long toEpochMillis(Object value, boolean isDate) {
        if (isDate) {
            if (value instanceof String isoDate) {
                return Instant.parse(isoDate).toEpochMilli();
            }
            throw new IllegalArgumentException("expected date value");
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        throw new IllegalArgumentException("expected numeric timestamp");
    }

    static void checkRequiredColumns(
        List<? extends ColumnInfo> columns,
        String timeField,
        String requiredSummaryCountField,
        @Nullable String datafeedId
    ) {
        boolean foundTimeField = false;
        boolean foundSummaryCountField = requiredSummaryCountField == null;
        for (ColumnInfo column : columns) {
            String name = column.name();
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
            if (requiredSummaryCountField != null && requiredSummaryCountField.equals(name)) {
                foundSummaryCountField = true;
            }
        }
        if (foundTimeField == false || foundSummaryCountField == false) {
            // Degrades gracefully when datafeedId is null, mirroring warnForConflictingOuterClauses's datafeedContext:
            // this validator also runs during PUT-time validation before a datafeed ID may exist yet.
            String datafeedContext = datafeedId == null ? "" : " for datafeed [" + datafeedId + "]";
            if (foundTimeField == false && foundSummaryCountField == false) {
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.DATAFEED_ESQL_MISSING_TIME_COLUMN, timeField, datafeedContext)
                        + " "
                        + Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_MISSING_SUMMARY_COUNT_COLUMN, requiredSummaryCountField)
                );
            }
            if (foundTimeField == false) {
                throw new IllegalArgumentException(
                    Messages.getMessage(Messages.DATAFEED_ESQL_MISSING_TIME_COLUMN, timeField, datafeedContext)
                );
            }
            throw new IllegalArgumentException(
                Messages.getMessage(Messages.DATAFEED_ESQL_DELAYED_DATA_MISSING_SUMMARY_COUNT_COLUMN, requiredSummaryCountField)
            );
        }
    }

    @SuppressWarnings("unchecked")
    protected void executeEsqlQueryAsync(
        Client client,
        String query,
        Map<String, String> headers,
        @Nullable String projectRouting,
        ActionListener<EsqlQueryResponse> listener
    ) {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = (EsqlQueryRequestBuilder<
            EsqlQueryRequest,
            EsqlQueryResponse>) EsqlQueryRequestBuilder.newRequestBuilder(client).query(query).allowPartialResults(false);
        if (projectRouting != null) {
            builder.projectRouting(projectRouting);
        }
        ClientHelper.executeWithHeadersAsync(
            client.threadPool().getThreadContext(),
            headers,
            ML_ORIGIN,
            builder,
            listener,
            (b, l) -> b.execute(l)
        );
    }
}
